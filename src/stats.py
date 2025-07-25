"""This module is used to provide overall storage stats for the MWA Archive"""

import argparse
import json
import logging
import multiprocessing as mp
import random
import subprocess
import time
from configparser import ConfigParser
from datetime import datetime
import csv
import boto3
import pyvo as vo
import psycopg2.pool
from dateutil.relativedelta import relativedelta
import numpy as np
import matplotlib.pyplot as plt


DPI = 100


logger = logging.getLogger("archive_stats")
logger.setLevel(logging.DEBUG)
console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
logger.addHandler(console_log)


def get_s3_resource(profile, endpoint_url) -> boto3.resource:
    """Given a profile and endpoint_url return a resource"""
    session = boto3.Session(profile_name=profile)
    return session.resource("s3", endpoint_url=endpoint_url)


def run_mc_du(profile: str, bucket_name: str) -> int:
    """Runs mc and appends output to filename"""
    cmd = f"/home/gsleap/minio-binaries/mc du {profile}/{bucket_name} --json"

    logger.info(f"{cmd}...")

    json_output = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        shell=True,
    ).stdout.decode("utf-8")

    mc_output = json.loads(json_output)

    # Example output:
    # ./mc du banksia/ingesttest --json
    # {
    # "prefix": "ingesttest",
    # "size": 8589934592,
    # "objects": 1,
    # "status": "success",
    # "isVersions": false
    # }
    size_bytes = int(mc_output["size"])

    logger.info(f"{cmd} == {size_bytes} bytes {bytes_to_terabytes(size_bytes):.3f} TB")

    return size_bytes


def get_acacia_usage(profile, endpoint_url) -> int:
    """
    Returns the bytes used from the S3 endpoint
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

    total_size = 0

    s3_resource = get_s3_resource(profile, endpoint_url)

    bucket_list = [bucket.name for bucket in s3_resource.buckets.all()]
    values = [(profile, bucket) for bucket in bucket_list]

    with mp.Pool(cpu_count) as pool:
        results = pool.starmap(run_mc_du, values)

    # bucket_size = sum(key.size for key in bucket.objects.all())
    for result in results:
        total_size += result

    return total_size


def randomise_banksia_vss_url(banksia_vss_endpoints: list) -> str:
    """Randomly chooses a VSS from the list of possible ones read from the config file"""
    vss = random.randint(0, len(banksia_vss_endpoints) - 1)
    return banksia_vss_endpoints[vss]


def randomise_banksia_vss_profile(banksia_vss_endpoints: list) -> str:
    """Randomly chooses a VSS from the list of possible ones read from the config file
    and then return "banksiaN" where N is the vss-N from the URL of the randomly
    chosen URL (13 character in the URL)"""
    vss = random.randint(0, len(banksia_vss_endpoints) - 1)
    # Get the vss number- from: https://vss-1.pawsey.org.au:9000 it would be 1.

    return f"banksia{banksia_vss_endpoints[vss][12]}"


def get_banksia_usage(aws_profile, endpoint_urls: list):
    """
    Returns the bytes used from the S3 endpoint
    as DMF, banksia

    aws_profile is a profile in ~/.aws/config
    not to be confused with the minIO client profile which is
    in .mc/config.json
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

    dmf_total_size = 0
    banksia_total_size = 0

    s3_resource = get_s3_resource(aws_profile, randomise_banksia_vss_url(endpoint_urls))

    bucket_list = [bucket.name for bucket in s3_resource.buckets.all()]
    dmf_buckets = []
    banksia_buckets = []

    for bucket in bucket_list:
        if (
            ("mwa01fs" in bucket)
            or ("mwa02fs" in bucket)
            or ("mwa03fs" in bucket)
            or ("mwa04fs" in bucket)
            or ("volt01fs" in bucket)
        ):
            dmf_buckets.append(bucket)
        elif "mwaingest" in bucket:
            banksia_buckets.append(bucket)
        else:
            logger.info(f"Skipping bucket {bucket}")

    # here we are creating a list of tuples to pass to the mp.Pool to execute
    # in THIS case the "profile" is a MinIO profile! In mc, the profile defines
    # both the credentials AND the endpoint, so here we want to randomise the profile
    # so we run mc against different VSS's so we don't kill Banksia!
    dmf_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket) for bucket in dmf_buckets]
    banksia_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket) for bucket in banksia_buckets]

    with mp.Pool(cpu_count) as pool:
        banksia_results = pool.starmap(run_mc_du, banksia_values)

    for banksia_result in banksia_results:
        banksia_total_size += banksia_result

    with mp.Pool(cpu_count) as pool:
        dmf_results = pool.starmap(run_mc_du, dmf_values)

    for dmf_result in dmf_results:
        dmf_total_size += dmf_result

    return dmf_total_size, banksia_total_size


def get_location_summary_bytes(mwa_db):
    """
    Returns the bytes stored for dmf, acacia and banksia
    from the database
    """
    conn = None
    results = None

    acacia_mwaingest = 0
    acacia_mwa = 0
    banksia = 0

    try:
        conn = mwa_db.getconn()
        cursor = conn.cursor()
        logger.info("Running big query to get location stats... please wait!")
        cursor.execute(
            """SELECT
            case
            when location IN (1, 3) then
                case bucket
                when 'mwa01fs' then 'DMF'
                when 'mwa02fs' then 'DMF'
                when 'mwa03fs' then 'DMF'
                when 'mwa04fs' then 'DMF'
                when 'volt01fs' then 'DMF'
                else 'Banksia' END
            when location =2 then 'Acacia_mwaingest'
            when location = 4 then 'Acacia_mwa' END As Location
            ,sum(size)
            FROM data_files
            WHERE deleted_timestamp is null and remote_archived=true
            GROUP BY 1""",
        )

        results = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)
    finally:
        if conn is not None:
            mwa_db.putconn(conn)

    if len(results) == 3 or len(results) == 4:
        for row in results:
            if row[0] == "DMF":
                dmf = row[1]
            elif row[0] == "Acacia_mwaingest":
                acacia_mwaingest = row[1]
            elif row[0] == "Acacia_mwa":
                acacia_mwa = row[1]
            elif row[0] == "Banksia":
                banksia = row[1]
            else:
                logger.info("Unexpected value!")
                exit(-1)
    else:
        logger.info("Error wrong number of rows!")
        exit(-1)

    return dmf, acacia_mwaingest, acacia_mwa, banksia


def do_query(vo_service, adql_statement):
    """Given a VO service object, run the ADQL and return the results"""
    results = vo_service.search(adql_statement)
    return results


def dump_stats(vo_service, filename):
    """Run an ADQL query to get stats and write them to a CSV file"""
    i = 0

    with open(filename, mode="w", encoding="utf-8") as stats_csv_file:
        stats_csv_writer = csv.writer(
            stats_csv_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        header = (
            "date",
            "projid",
            "config",
            "time(s)",
            "archived(bytes)",
            "deleted(bytes)",
            "time(hours)",
            "archived(TB)",
        )

        stats_csv_writer.writerow(header)

        total_bytes = 0.0
        deleted_bytes = 0.0
        total_secs = 0.0

        results = do_query(
            vo_service,
            """SELECT
                    date_trunc('day', starttime_utc) As reporting_date
                ,projectid
                ,mwa_array_configuration
                ,SUM(duration) as total_time_secs
                ,SUM(total_archived_bytes) as total_archived_bytes
                ,SUM(files_deleted_bytes) as deleted_bytes
            FROM mwa.observation
            GROUP BY 1,2,3
            ORDER BY 1,2""",
        )

        for row in results:
            i = i + 1

            if not row["total_time_secs"] is None:
                total_secs += int(row["total_time_secs"])
                hours = int(row["total_time_secs"]) / 3600
            else:
                hours = 0.0

            if not row["total_archived_bytes"] is None:
                this_bytes = int(row["total_archived_bytes"])
                total_bytes += this_bytes
                terabytes = bytes_to_terabytes(this_bytes)
            else:
                terabytes = 0.0

            if not row["deleted_bytes"] is None:
                deleted_bytes += int(row["deleted_bytes"])

            stats_csv_writer.writerow(
                (
                    row["reporting_date"],
                    row["projectid"],
                    row["mwa_array_configuration"],
                    int(row["total_time_secs"]),
                    int(row["total_archived_bytes"]),
                    int(row["deleted_bytes"]),
                    hours,
                    terabytes,
                )
            )

    logger.info(f"{i} rows written to {filename}.\n")
    logger.info(f"Total data: { bytes_to_petabytes(total_bytes) } PB\n")
    logger.info(f"Total time: { total_secs / 3600 } hours\n")
    logger.info(f"Total deleted data: { bytes_to_petabytes(deleted_bytes) } PB\n")


def dump_stats_by_project(local_db_conn, filename):
    """Dumps stats grouped by project to a CSV file"""
    i = 0

    with open(filename, mode="w", encoding="utf-8") as stats_csv_file:
        stats_csv_writer = csv.writer(
            stats_csv_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        header = ("projid", "projname", "TB")

        stats_csv_writer.writerow(header)

        results = do_query(
            local_db_conn,
            """SELECT
                    projectid
                    ,projectshortname
                    ,SUM(total_archived_bytes) As total_archived_bytes
            FROM mwa.observation
            GROUP BY projectid,
                    projectshortname
            ORDER BY 3 DESC""",
        )

        for row in results:
            i = i + 1

            # lookup project description
            projid = row["projectid"]
            projname = row["projectshortname"]

            terabytes = bytes_to_terabytes(int(row["total_archived_bytes"]))
            stats_csv_writer.writerow(
                (
                    projid,
                    projname,
                    terabytes,
                )
            )

    logger.info(f"{i} rows written to {filename}.\n")


def dump_monthly_stats(vo_service, filename):
    """Dump stats by month to a CSV file"""
    i = 0

    with open(filename, mode="w", encoding="utf-8") as stats_csv_file:
        stats_csv_writer = csv.writer(
            stats_csv_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        header = ("year", "month", "hrs", "TB", "avail_hrs", "duty_cycle")
        stats_csv_writer.writerow(header)

        results = do_query(
            vo_service,
            """SELECT
                    date_part('year', date_trunc('day', starttime_utc)) as reporting_year
                    ,date_part('month', date_trunc('day', starttime_utc)) as reporting_month
                    ,SUM(duration) as month_secs
                    ,SUM(total_archived_bytes) as month_bytes
                FROM mwa.observation
                GROUP BY 1,2
                ORDER BY 1,2""",
        )

        for row in results:
            i = i + 1
            year = int(row["reporting_year"])
            month = int(row["reporting_month"])
            hours = row["month_secs"] / 3600
            data_bytes = row["month_bytes"]

            terabytes = bytes_to_terabytes(data_bytes)
            available_hours = get_available_hours(year, month)
            duty_cycle = get_duty_cycle(hours, available_hours)

            csv_row = (
                year,
                month,
                hours,
                terabytes,
                available_hours,
                duty_cycle,
            )

            stats_csv_writer.writerow(csv_row)

        logger.info(f"{i} rows written to {filename}.\n")


def get_filetype_by_id(filetype_id):
    """Return a filetype name given an id"""
    types = [
        "Unknown (0)",
        "Raw VSIB burst",
        "Averaged VSIB burst",
        "Instrument config",
        "header.txt file",
        "Instrument config header",
        "lacspc",
        "lccspc",
        "Raw Correlator fits",
        "Antenna config header",
        "MWA Flag File",
        "Raw Voltage",
        "Raw Voltage Recombined",
        "uvfits",
        "metafits PPD File",
        "Voltage ICS",
        "Voltage Recombined TAR",
    ]
    try:
        return types[filetype_id]
    except IndexError:
        return f"Unknown ({filetype_id})"


def get_duty_cycle(hours, available_hours):
    """Calculate duty cycle"""
    if available_hours > 0:
        return hours / available_hours
    else:
        return 0


def get_available_hours(year: int, month: int):
    """Calculate available hours in a month"""
    start_date = datetime(year, month, 1)

    # get end date.
    month = month + 1
    if month == 13:
        month = 1
        year = year + 1

    end_date = datetime(year, month, 1)

    # return hours
    return (end_date - start_date).total_seconds() / 3600


def clear_plots():
    """Clear plots"""
    fig = plt.figure()

    if fig:
        fig.clear()
        plt.close(fig)


def get_deleted_data_by_month(mwa_db, date_from, date_to):
    """Get the deleted data by month from a query"""
    conn = None
    results = None

    try:
        conn = mwa_db.getconn()
        cursor = conn.cursor()
        logger.info("Running big query to get deleted data stats per month... please wait!")
        cursor.execute(
            """
            SELECT
                    date_part('year', date_trunc('day', deleted_timestamp)) as reporting_year
                ,date_part('month', date_trunc('day', deleted_timestamp)) as reporting_month
                ,SUM(size) as deleted_bytes
            FROM data_files
            WHERE
                    deleted_timestamp BETWEEN %s AND %s
            GROUP BY 1,2
            ORDER BY 1,2
            """,
            (date_from, date_to),
        )

        results = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)
    finally:
        if conn is not None:
            mwa_db.putconn(conn)

    return results


def do_plot_archive_volume_per_month(
    tap_service,
    mwa_db,
    date_from,
    date_to,
    title,
    cumulative,
    filename,
    ingest_only,
    dump_year_from,
    dump_year_to,
    dump_month_from,
    dump_month_to,
):
    """Plot archive volume per month"""
    clear_plots()

    x_axis = []
    y_axis = []
    cumulative_volume_bytes = 0

    # If showing more than 6 months, make the stride longer
    if (date_to - date_from).days > (6 * 31):
        stride_months = 3
    else:
        stride_months = 1

    results = do_query(
        tap_service,
        f"""SELECT
                date_part('year', date_trunc('day', starttime_utc)) as reporting_year
                ,date_part('month', date_trunc('day', starttime_utc)) as reporting_month
                ,SUM(total_archived_bytes + files_deleted_bytes) as total_data_bytes
            FROM mwa.observation
            WHERE
                starttime_utc BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY 1,2
            ORDER BY 1,2 """,
    )
    deleted_results = None
    if not ingest_only:
        deleted_results = get_deleted_data_by_month(mwa_db, date_from, date_to)

    for row in results:
        this_bytes = int(row["total_data_bytes"])
        this_deleted_bytes = 0
        cumulative_volume_bytes += this_bytes

        if not ingest_only:
            # find row in deleted_results
            # col 0 = Reporting Year
            # col 1 = Reporting Month
            # col 2 = sum(data deleted bytes)
            for drow in deleted_results:
                if row["reporting_year"] == drow[0] and row["reporting_month"] == drow[1]:
                    deleted_bytes = int(drow[2])
                    this_bytes -= deleted_bytes
                    this_deleted_bytes = deleted_bytes
                    cumulative_volume_bytes -= deleted_bytes

        volume_bytes = this_bytes

        # Check striding
        if row["reporting_month"] % stride_months == 0:
            x_axis.append(f'{int(row["reporting_year"]):d}-{int(row["reporting_month"]):02d}')

            if cumulative:
                y_axis.append(bytes_to_terabytes(cumulative_volume_bytes))
            else:
                y_axis.append(bytes_to_terabytes(volume_bytes))

        # Only dump this debug to the screen if we are including deleted data and
        # in the year and qtrs we want and only if this code is being run on the
        # full archive and not just 6 months worth. This dump code is here
        # because it is convenient - it should be moved into a seperate module really
        if (
            not ingest_only
            and (date_to - date_from).days > (31 * 6)
            and row["reporting_year"] >= dump_year_from
            and row["reporting_year"] <= dump_year_to
            and row["reporting_month"] >= dump_month_from
            and row["reporting_month"] <= dump_month_to
        ):
            logger.info("year, month, ingested-deleted, ingested, deleted, cuml" " archive volume(all in TB)")

            logger.info(
                f"{row['reporting_year']},{row['reporting_month']},{bytes_to_terabytes(volume_bytes):.3f},"
                f"{bytes_to_terabytes(volume_bytes + this_deleted_bytes):.3f},"
                f"{bytes_to_terabytes(this_deleted_bytes):.3f},"
                f"{bytes_to_terabytes(cumulative_volume_bytes):.3f}"
            )

    volume_petabytes = bytes_to_petabytes(cumulative_volume_bytes)

    fig, _ = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title(f"{title} = {volume_petabytes:.3f} PB (as at {time.strftime('%d-%b-%Y')})")
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Terabytes (TB)")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)


def do_plot_archive_volume_per_project(tap_service, date_from, date_to, title, filename):
    """Plot archive volume per project"""
    clear_plots()

    labels = []
    x_values = []
    slice_no = 0
    max_slices = 11
    other_bytes = 0

    results = do_query(
        tap_service,
        f"""SELECT projectid,
                projectshortname,
                COALESCE(SUM(total_archived_bytes),0) as total_archived_bytes
            FROM mwa.observation
            WHERE
                starttime_utc BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY projectid,
                    projectshortname
            ORDER BY 3 DESC""",
    )

    for row in results:
        value_bytes = int(row["total_archived_bytes"])

        if slice_no >= max_slices:
            other_bytes += value_bytes
        else:
            x_values.append(bytes_to_terabytes(value_bytes))
            labels.append(f"{row['projectid']}-{row['projectshortname']}")

        slice_no += 1

    # Add 'Other'
    labels.append("Other")
    x_values.append(bytes_to_terabytes(other_bytes))

    fig, axis = plt.subplots()
    axis.pie(
        x_values,
        labels=labels,
        autopct=lambda pct: pie_volume_format(pct, x_values),
        startangle=0,
    )
    axis.axis("equal")

    plt.title(f"{title} (as at {time.strftime('%d-%b-%Y')})")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(
        filename,
        dpi=DPI,
    )


def do_plot_telescope_time_per_project(tap_service, date_from, date_to, title, filename):
    """Plot telescope time per project"""
    clear_plots()

    labels = []
    x_values = []
    slice_no = 0
    max_slices = 8
    other_time = 0

    results = do_query(
        tap_service,
        f"""SELECT projectid,
                projectshortname,
                COALESCE(SUM(duration),0)/3600 As totaltime_hours
            FROM mwa.observation
            WHERE
                starttime_utc BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY projectid,
                    projectshortname
            ORDER BY 3 DESC""",
    )

    for row in results:
        if slice_no >= max_slices:
            other_time += int(row["totaltime_hours"])
        else:
            labels.append(f"{row['projectid']}-{row['projectshortname']}")
            x_values.append(int(row["totaltime_hours"]))

        slice_no += 1

    # Add 'Other'
    labels.append("Other")
    x_values.append(other_time)

    fig, axis = plt.subplots()
    axis.pie(
        x_values,
        labels=labels,
        autopct=lambda pct: pie_hours_format(pct, x_values),
        startangle=0,
    )
    axis.axis("equal")
    plt.title(f"{title} by Project (as at {time.strftime('%d-%b-%Y')})")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)


def pie_hours_format(pct, allvals):
    """Format percentage labels for pie chart"""
    absolute = int(pct / 100.0 * float(np.sum(allvals)))

    if pct < 5:
        return f"{pct:.1f}%"
    else:
        return f"{pct:.1f}%\n({absolute:d} hrs)"


def pie_volume_format(pct, allvals):
    """Format volume"""
    absolute = int(pct / 100.0 * float(np.sum(allvals)))

    if pct < 5:
        return f"{pct:.1f}%"
    else:
        return f"{pct:.1f}%\n({absolute:d} TB)"


def bytes_to_terabytes(bytes_value):
    """Convert bytes to TB"""
    if bytes_value is None:
        return 0.0
    else:
        return float(bytes_value) / (1000.0 * 1000.0 * 1000.0 * 1000.0)


def bytes_to_petabytes(bytes_value):
    """Convert bytes to PB"""
    if bytes_value is None:
        return 0.0
    else:
        return float(bytes_value) / (1000.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0)


def run_stats(config_filename):
    """Main function"""
    # Usage: python stats.py --cfg=path/to/config/file

    config = ConfigParser()
    config.read(config_filename)

    acacia_mwaingest_quota_bytes = config.getint("asvo_stats", "acacia_mwaingest_quota_bytes")
    acacia_mwa_quota_bytes = config.getint("asvo_stats", "acacia_mwa_quota_bytes")
    banksia_quota_bytes = config.getint("asvo_stats", "banksia_quota_bytes")

    tap_url = config.get("MWA TAP", "url")
    mwa_tap_service = vo.dal.TAPService(tap_url)

    # Example of the expected format is: 01-Jul-2022
    special_date_from = datetime.strptime(config.get("asvo_stats", "special_date_from"), "%d-%b-%Y")
    special_date_to = datetime.strptime(config.get("asvo_stats", "special_date_to"), "%d-%b-%Y")

    mwa_db = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=2,
        host=config.get("MWA Database", "dbhost"),
        user=config.get("MWA Database", "dbuser"),
        database=config.get("MWA Database", "dbname"),
        password=config.get("MWA Database", "dbpass"),
        port=config.getint("MWA Database", "dbport"),
    )

    today = datetime.today()
    start_date = datetime(2006, 1, 1)

    six_months_ago = today - relativedelta(months=6)

    # Get acacia and banksia totals from S3
    if config.getint("S3", "use_acacia_mwaingest") == 1:
        acacia_mwaingest_profile = config.get("S3", "acacia_mwaingest_profile")
        acacia_mwaingest_endpoint_url = config.get("S3", "acacia_mwaingest_endpoint_url")

        logger.info("Getting stats from Acacia_mwaingest...")
        acacia_mwaingest_bytes = get_acacia_usage(acacia_mwaingest_profile, acacia_mwaingest_endpoint_url)
    else:
        logger.info("Skipping stats from Acacia_mwaingest (use_acacia_mwaingest != 1)")
        acacia_mwaingest_bytes = 0

    if config.getint("S3", "use_acacia_mwa") == 1:
        acacia_mwa_profile = config.get("S3", "acacia_mwa_profile")
        acacia_mwa_endpoint_url = config.get("S3", "acacia_mwa_endpoint_url")

        logger.info("Getting stats from Acacia_mwa...")
        acacia_mwa_bytes = get_acacia_usage(acacia_mwa_profile, acacia_mwa_endpoint_url)
    else:
        logger.info("Skipping stats from Acacia_mwa (use_acacia_mwa != 1)")
        acacia_mwa_bytes = 0

    if config.getint("S3", "use_banksia") == 1:
        banksia_profile = config.get("S3", "banksia_profile")
        banksia_endpoint_urls = str(config.get("S3", "banksia_endpoint_urls")).split(",")

        logger.info("Getting stats from Banksia...")
        dmf_bytes, banksia_bytes = get_banksia_usage(banksia_profile, banksia_endpoint_urls)
    else:
        logger.info("Skipping stats from Banksia (use_banksia != 1)")
        dmf_bytes = 0
        banksia_bytes = 0

    logger.info("Getting summary stats from database...")
    (
        db_dmf_bytes,
        db_acacia_mwaingest_bytes,
        db_acacia_mwa_bytes,
        db_banksia_bytes,
    ) = get_location_summary_bytes(mwa_db)

    logger.info(
        f"Acacia mwaingest vs DB         : {bytes_to_terabytes(acacia_mwaingest_bytes)} TB vs"
        f" {bytes_to_terabytes(db_acacia_mwaingest_bytes)} TB"
    )
    logger.info(
        f"Acacia mwa       vs DB         : {bytes_to_terabytes(acacia_mwa_bytes)} TB vs"
        f" {bytes_to_terabytes(db_acacia_mwa_bytes)} TB"
    )
    logger.info(
        f"DMF vs DB            : {bytes_to_terabytes(dmf_bytes)} TB vs" f" {bytes_to_terabytes(db_dmf_bytes)} TB"
    )
    logger.info(
        f"Banksia vs DB        : {bytes_to_terabytes(banksia_bytes)} TB vs"
        f" {bytes_to_terabytes(db_banksia_bytes)} TB"
    )
    logger.info("--------------------------------------------------")
    logger.info(
        f"Total Banksia vs DB  : {bytes_to_terabytes(dmf_bytes+banksia_bytes)} TB"
        f" vs {bytes_to_terabytes(db_dmf_bytes + db_banksia_bytes)} TB"
    )
    logger.info("--------------------------------------------------")
    logger.info(
        "Total Pawsey LTS vs DB:"
        f" {bytes_to_terabytes(acacia_mwaingest_bytes + acacia_mwa_bytes + dmf_bytes + banksia_bytes)} TB vs"
        f" {bytes_to_terabytes(db_acacia_mwaingest_bytes + db_acacia_mwa_bytes + db_dmf_bytes + db_banksia_bytes)} TB"
    )
    logger.info("--------------------------------------------------")
    acacia_mwaingest_percent_used = ((acacia_mwaingest_bytes) / acacia_mwaingest_quota_bytes) * 100.0
    logger.info(
        "Acacia_mwaingest Quota Used:"
        f" {bytes_to_terabytes(acacia_mwaingest_bytes):.3f} TB /"
        f" {bytes_to_terabytes(acacia_mwaingest_quota_bytes):.3f} TB == "
        f" {acacia_mwaingest_percent_used:.1f} % used"
    )
    acacia_mwa_percent_used = ((acacia_mwa_bytes) / acacia_mwa_quota_bytes) * 100.0
    logger.info(
        "Acacia_mwa Quota Used:"
        f" {bytes_to_terabytes(acacia_mwa_bytes):.3f} TB /"
        f" {bytes_to_terabytes(acacia_mwa_quota_bytes):.3f} TB == "
        f" {acacia_mwa_percent_used:.1f} % used"
    )
    banksia_percent_used = ((dmf_bytes + banksia_bytes) / banksia_quota_bytes) * 100.0
    logger.info(
        "Banksia Quota Used:"
        f" {bytes_to_terabytes(dmf_bytes + banksia_bytes):.3f} TB /"
        f" {bytes_to_terabytes(banksia_quota_bytes):.3f} TB == "
        f" {banksia_percent_used:.1f} % used"
    )
    lts_percent_used = (
        (acacia_mwaingest_bytes + acacia_mwa_bytes + dmf_bytes + banksia_bytes)
        / (acacia_mwaingest_quota_bytes + acacia_mwa_quota_bytes + banksia_quota_bytes)
    ) * 100
    logger.info(
        "Pawsey Quota Used:"
        f" {bytes_to_terabytes(acacia_mwaingest_bytes + acacia_mwa_bytes + dmf_bytes + banksia_bytes):.3f} TB"
        f" / {bytes_to_terabytes(acacia_mwaingest_quota_bytes + acacia_mwa_quota_bytes + banksia_quota_bytes):.3f} TB"
        f" == {lts_percent_used:.1f} % used"
    )

    logger.info("\n-------------------------------------------------------")

    acacia_mwaingest_available_bytes = acacia_mwaingest_quota_bytes - acacia_mwaingest_bytes
    acacia_mwa_available_bytes = acacia_mwa_quota_bytes - acacia_mwa_bytes
    banksia_available_bytes = banksia_quota_bytes - (dmf_bytes + banksia_bytes)
    pawsey_available_bytes = acacia_mwaingest_available_bytes + acacia_mwa_available_bytes + banksia_available_bytes

    logger.info(f"Acacia_mwaingest Quota Available: {bytes_to_terabytes(acacia_mwaingest_available_bytes):.3f} TB ")
    logger.info(f"Acacia_mwa Quota Available      : {bytes_to_terabytes(acacia_mwa_available_bytes):.3f} TB ")
    logger.info(f"Banksia Quota Available         : {bytes_to_terabytes(banksia_available_bytes):.3f} TB ")
    logger.info(f"Pawsey Quota Available          : {bytes_to_terabytes(pawsey_available_bytes):.3f} TB ")
    logger.info("-------------------------------------------------------\n")

    # Either way show whats in the db
    dump_stats(mwa_tap_service, "stats.csv")
    dump_monthly_stats(mwa_tap_service, "stats_by_month.csv")
    dump_stats_by_project(mwa_tap_service, "stats_by_project.csv")

    # special stats get dumped for the quarterly report to AAL
    dump_year_from = special_date_from.year
    dump_year_to = special_date_to.year
    dump_month_from = special_date_from.month
    dump_month_to = special_date_to.month

    do_plot_archive_volume_per_month(
        mwa_tap_service,
        mwa_db,
        start_date,
        today,
        "MWA Archive Volume (all time)",
        True,
        "mwa_archive_volume_all_time.png",
        False,
        dump_year_from,
        dump_year_to,
        dump_month_from,
        dump_month_to,
    )
    do_plot_archive_volume_per_month(
        mwa_tap_service,
        mwa_db,
        start_date,
        today,
        "MWA Archive Ingest (all time)",
        True,
        "mwa_archive_ingest_all_time.png",
        True,
        None,
        None,
        None,
        None,
    )
    do_plot_archive_volume_per_project(
        mwa_tap_service,
        start_date,
        today,
        "MWA Archive Volume by Project (all time)",
        "mwa_archive_volume_by_project_all_time.png",
    )
    do_plot_telescope_time_per_project(
        mwa_tap_service,
        start_date,
        today,
        "MWA Telescope Time (all time)",
        "mwa_telescope_time_all_time.png",
    )

    do_plot_archive_volume_per_month(
        mwa_tap_service,
        mwa_db,
        six_months_ago,
        today,
        "MWA Archive Net Growth (last 6 months)",
        False,
        "mwa_archive_net_growth_last_6_months.png",
        False,
        dump_year_from,
        dump_year_to,
        dump_month_from,
        dump_month_to,
    )
    do_plot_archive_volume_per_month(
        mwa_tap_service,
        mwa_db,
        six_months_ago,
        today,
        "MWA Archive Ingest (last 6 months)",
        False,
        "mwa_archive_ingest_last_6_months.png",
        True,
        None,
        None,
        None,
        None,
    )
    do_plot_archive_volume_per_project(
        mwa_tap_service,
        six_months_ago,
        today,
        "MWA Archive Volume by Project (last 6 months)",
        "mwa_archive_volume_by_project_last_6_months.png",
    )
    do_plot_telescope_time_per_project(
        mwa_tap_service,
        six_months_ago,
        today,
        "MWA Telescope Time (last 6 months)",
        "mwa_telescope_time_last_6_months.png",
    )


if __name__ == "__main__":
    # Get command line args
    parser = argparse.ArgumentParser()
    parser.description = (
        "archive_stats calculates usage from info dervied"
        " from the MWA database, TAP service, Acacia and"
        " Banksia. Needs to be run from a machine with access"
        " to Acacia/Banksia and the MWA database.\n"
    )

    parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

    args = vars(parser.parse_args())

    run_stats(args["cfg"])
