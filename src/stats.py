"""This module is used to provide overall storage stats for the MWA Archive"""

import argparse
import csv
import json
import logging
import multiprocessing as mp
import os
import random
import subprocess
import sys
import time
from configparser import ConfigParser
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import psycopg2.errors
import psycopg2.pool
import pyvo as vo
from dateutil.relativedelta import relativedelta

DPI = 100
AWST = ZoneInfo("Australia/Perth")

logger = logging.getLogger("archive_stats")
logger.setLevel(logging.DEBUG)
console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
logger.addHandler(console_log)


# ---------------------------------------------------------------------------
# S3 / MinIO helpers
# ---------------------------------------------------------------------------


def get_s3_resource(profile, endpoint_url):
    """Given a profile and endpoint_url return a resource"""
    session = boto3.Session(profile_name=profile)
    return session.resource("s3", endpoint_url=endpoint_url)


def run_mc_du(profile: str, bucket_name: str, minio_path: str) -> int:
    """Runs mc and appends output to filename"""
    cmd = f"{minio_path} du {profile}/{bucket_name} --json"

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


def get_acacia_usage(profile, endpoint_url, minio_path) -> int:
    """
    Returns the bytes used from the S3 endpoint
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

    s3_resource = get_s3_resource(profile, endpoint_url)

    bucket_list = [bucket.name for bucket in s3_resource.buckets.all()]
    values = [(profile, bucket, minio_path) for bucket in bucket_list]

    with mp.Pool(cpu_count) as pool:
        results = pool.starmap(run_mc_du, values)

    return sum(results)


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


def get_banksia_usage(aws_profile, endpoint_urls: list, minio_path):
    """
    Returns the bytes used from the S3 endpoint
    as DMF, banksia

    aws_profile is a profile in ~/.aws/config
    not to be confused with the minIO client profile which is
    in .mc/config.json
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

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
    dmf_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket, minio_path) for bucket in dmf_buckets]
    banksia_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket, minio_path) for bucket in banksia_buckets]

    with mp.Pool(cpu_count) as pool:
        banksia_results = pool.starmap(run_mc_du, banksia_values)

    with mp.Pool(cpu_count) as pool:
        dmf_results = pool.starmap(run_mc_du, dmf_values)

    return sum(banksia_results) + sum(dmf_results)


# ---------------------------------------------------------------------------
# Data fetching — each query runs once
# ---------------------------------------------------------------------------


def do_query(vo_service, adql_statement):
    """Given a VO service object, run the ADQL and return the results"""
    return vo_service.search(adql_statement)


def fetch_monthly_data(tap_service):
    """Fetch all monthly aggregates in a single TAP query (no date filter).

    Returns a list of dicts with keys:
        reporting_year, reporting_month, total_secs,
        total_archived_bytes, files_deleted_bytes,
        all_ingested_data_bytes, downloaded_bytes
    """
    logger.info("Fetching monthly data from TAP...")
    results = do_query(
        tap_service,
        """SELECT
                date_part('year', date_trunc('day', starttime_utc)) as reporting_year
                ,date_part('month', date_trunc('day', starttime_utc)) as reporting_month
                ,COALESCE(SUM(duration), 0) as total_secs
                ,COALESCE(SUM(total_archived_bytes), 0) as total_archived_bytes
                ,COALESCE(SUM(files_deleted_bytes), 0) as files_deleted_bytes
                ,COALESCE(SUM(total_archived_bytes + files_deleted_bytes), 0) as all_ingested_data_bytes
                ,COALESCE(SUM(downloaded_bytes), 0) as downloaded_bytes
            FROM mwa.observation
            GROUP BY 1, 2
            ORDER BY 1, 2""",
    )
    return [
        {
            "reporting_year": int(row["reporting_year"]),
            "reporting_month": int(row["reporting_month"]),
            "total_secs": int(row["total_secs"]),
            "total_archived_bytes": int(row["total_archived_bytes"]),
            "files_deleted_bytes": int(row["files_deleted_bytes"]),
            "all_ingested_data_bytes": int(row["all_ingested_data_bytes"]),
            "downloaded_bytes": int(row["downloaded_bytes"]),
        }
        for row in results
    ]


def fetch_project_data(tap_service, date_from=None, date_to=None):
    """Fetch project-level aggregates (bytes and hours).

    If date_from/date_to are provided, restrict to that range.
    Returns a list of dicts with keys:
        projectid, projectshortname, total_archived_bytes, totaltime_hours
    """
    where_clause = ""
    if date_from is not None and date_to is not None:
        where_clause = f"WHERE starttime_utc BETWEEN '{date_from}' AND '{date_to}'"

    label = f" ({date_from} to {date_to})" if date_from else " (all time)"
    logger.info(f"Fetching project data from TAP{label}...")

    results = do_query(
        tap_service,
        f"""SELECT
                projectid
                ,projectshortname
                ,COALESCE(SUM(total_archived_bytes), 0) as total_archived_bytes
                ,COALESCE(SUM(duration), 0) / 3600 as totaltime_hours
            FROM mwa.observation
            {where_clause}
            GROUP BY projectid, projectshortname""",
    )
    return [
        {
            "projectid": row["projectid"],
            "projectshortname": row["projectshortname"],
            "total_archived_bytes": int(row["total_archived_bytes"]),
            "totaltime_hours": int(row["totaltime_hours"]),
        }
        for row in results
    ]


def fetch_daily_stats(tap_service):
    """Fetch daily stats by project/config for the CSV dump.

    This is the finest-grained query and can't be shared with plots,
    so it remains its own TAP call.
    """
    logger.info("Fetching daily stats from TAP...")
    return do_query(
        tap_service,
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


def fetch_deleted_data_by_month(mwa_db):
    """Fetch all deleted-data monthly aggregates from the database (no date filter).

    Returns a list of dicts with keys: reporting_year, reporting_month, deleted_bytes
    """
    logger.info("Fetching deleted data by month from database...")
    conn = None
    results = None

    try:
        conn = mwa_db.getconn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                date_part('year', date_trunc('day', deleted_timestamp)) as reporting_year
                ,date_part('month', date_trunc('day', deleted_timestamp)) as reporting_month
                ,COALESCE(SUM(size), 0) as deleted_bytes
            FROM data_files
            WHERE deleted_timestamp IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        )
        results = cursor.fetchall()
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            mwa_db.putconn(conn)

    if results is None:
        return []

    return [
        {
            "reporting_year": int(row[0]),
            "reporting_month": int(row[1]),
            "deleted_bytes": int(row[2]),
        }
        for row in results
    ]


def get_location_summary_bytes(mwa_tap):
    """Returns the bytes stored for acacia and banksia from the database"""
    logger.info("Running query to get location stats...")
    results = do_query(
        mwa_tap,
        """SELECT SUM(archived_bytes_at_pawsey_banksia) as banksia,
                  SUM(archived_bytes_at_pawsey_acacia_mwa) as acacia_mwa
           FROM mwa.observation""",
    )

    if results:
        acacia_mwa = int(results[0]["acacia_mwa"])
        banksia = int(results[0]["banksia"])
    else:
        logger.error("No rows returned!")
        sys.exit(-1)

    return acacia_mwa, banksia


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------


def filter_monthly_by_range(monthly_data, date_from, date_to):
    """Filter monthly rows to those whose (year, month) falls within the date range."""
    from_ym = (date_from.year, date_from.month)
    to_ym = (date_to.year, date_to.month)
    return [row for row in monthly_data if from_ym <= (row["reporting_year"], row["reporting_month"]) <= to_ym]


def build_deleted_lookup(deleted_data):
    """Build a {(year, month): deleted_bytes} dict for O(1) lookups."""
    return {(row["reporting_year"], row["reporting_month"]): row["deleted_bytes"] for row in deleted_data}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


# def get_filetype_by_id(filetype_id):
#     """Return a filetype name given an id"""
#     types = [
#         "Unknown (0)",
#         "Raw VSIB burst",
#         "Averaged VSIB burst",
#         "Instrument config",
#         "header.txt file",
#         "Instrument config header",
#         "lacspc",
#         "lccspc",
#         "Raw Correlator fits",
#         "Antenna config header",
#         "MWA Flag File",
#         "Raw Voltage",
#         "Raw Voltage Recombined",
#         "uvfits",
#         "metafits PPD File",
#         "Voltage ICS",
#         "Voltage Recombined TAR",
#     ]
#     try:
#         return types[filetype_id]
#     except IndexError:
#         return f"Unknown ({filetype_id})"


def get_duty_cycle(hours, available_hours):
    """Calculate duty cycle"""
    if available_hours > 0:
        return hours / available_hours
    return 0


def get_available_hours(year: int, month: int):
    """Calculate available hours in a month"""
    start_date = datetime(year, month, 1, tzinfo=AWST)

    # get end date.
    month = month + 1
    if month == 13:
        month = 1
        year = year + 1

    end_date = datetime(year, month, 1, tzinfo=AWST)

    # return hours
    return (end_date - start_date).total_seconds() / 3600


def clear_plots():
    """Clear all open plots"""
    plt.close("all")


def bytes_to_terabytes(bytes_value):
    """Convert bytes to TB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / (1000.0**4)


def bytes_to_petabytes(bytes_value):
    """Convert bytes to PB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / (1000.0**5)


def pie_hours_format(pct, allvals):
    """Format percentage labels for pie chart"""
    absolute = int(pct / 100.0 * float(np.sum(allvals)))

    if pct < 5:
        return f"{pct:.1f}%"
    return f"{pct:.1f}%\n({absolute:d} hrs)"


def pie_volume_format(pct, allvals):
    """Format volume"""
    absolute = int(pct / 100.0 * float(np.sum(allvals)))

    if pct < 5:
        return f"{pct:.1f}%"
    return f"{pct:.1f}%\n({absolute:d} TB)"


# ---------------------------------------------------------------------------
# CSV dump functions — take pre-fetched data, not service handles
# ---------------------------------------------------------------------------


def dump_stats(daily_data, filename):
    """Write daily stats to a CSV file.

    daily_data is the raw pyvo result set from fetch_daily_stats().
    """
    i = 0
    total_bytes = 0.0
    deleted_bytes = 0.0
    total_secs = 0.0

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

        for row in daily_data:
            i += 1

            if row["total_time_secs"] is not None:
                total_secs += int(row["total_time_secs"])
                hours = int(row["total_time_secs"]) / 3600
            else:
                hours = 0.0

            if row["total_archived_bytes"] is not None:
                this_bytes = int(row["total_archived_bytes"])
                total_bytes += this_bytes
                terabytes = bytes_to_terabytes(this_bytes)
            else:
                terabytes = 0.0

            if row["deleted_bytes"] is not None:
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

    logger.info(f"{i} rows written to {filename}.")
    logger.info(f"Total data: {bytes_to_petabytes(total_bytes)} PB")
    logger.info(f"Total time: {total_secs / 3600} hours")
    logger.info(f"Total deleted data: {bytes_to_petabytes(deleted_bytes)} PB")


def dump_stats_by_project(project_data, filename):
    """Write project stats to a CSV file."""
    i = 0

    # Sort by archived bytes descending (consistent with original query)
    sorted_data = sorted(project_data, key=lambda r: r["total_archived_bytes"], reverse=True)

    with open(filename, mode="w", encoding="utf-8") as stats_csv_file:
        stats_csv_writer = csv.writer(
            stats_csv_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        stats_csv_writer.writerow(("projid", "projname", "TB"))

        for row in sorted_data:
            i += 1
            stats_csv_writer.writerow(
                (
                    row["projectid"],
                    row["projectshortname"],
                    bytes_to_terabytes(row["total_archived_bytes"]),
                )
            )

    logger.info(f"{i} rows written to {filename}.")


def dump_monthly_stats(monthly_data, filename):
    """Write monthly stats to a CSV file."""
    i = 0

    with open(filename, mode="w", encoding="utf-8") as stats_csv_file:
        stats_csv_writer = csv.writer(
            stats_csv_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        stats_csv_writer.writerow(("year", "month", "hrs", "TB", "avail_hrs", "duty_cycle"))

        for row in monthly_data:
            i += 1
            year = row["reporting_year"]
            month = row["reporting_month"]
            hours = row["total_secs"] / 3600
            terabytes = bytes_to_terabytes(row["total_archived_bytes"])
            available_hours = get_available_hours(year, month)
            duty_cycle = get_duty_cycle(hours, available_hours)

            stats_csv_writer.writerow(
                (
                    year,
                    month,
                    hours,
                    terabytes,
                    available_hours,
                    duty_cycle,
                )
            )

    logger.info(f"{i} rows written to {filename}.")


# ---------------------------------------------------------------------------
# Plotting functions — take pre-fetched data, not service handles
# ---------------------------------------------------------------------------


def plot_archive_volume_per_month(
    monthly_data,
    deleted_lookup,
    date_from,
    date_to,
    title,
    cumulative,
    filename,
    ingest_only,
):
    """Plot archive volume per month from pre-fetched data."""
    clear_plots()

    rows = filter_monthly_by_range(monthly_data, date_from, date_to)

    x_axis = []
    y_axis = []
    cumulative_volume_bytes = 0

    stride_months = 2 if (date_to - date_from).days > (6 * 31) else 1
    stride_accumulator = 0
    stride_count = 0

    for row in rows:
        net_bytes = row["all_ingested_data_bytes"]
        cumulative_volume_bytes += net_bytes

        if not ingest_only:
            key = (row["reporting_year"], row["reporting_month"])
            deleted_bytes = deleted_lookup.get(key, 0)
            net_bytes -= deleted_bytes
            cumulative_volume_bytes -= deleted_bytes

        stride_accumulator += net_bytes
        stride_count += 1

        # Check striding - step every N rows from start, not by calendar month parity
        if stride_count % stride_months == 0:
            x_axis.append(f"{row['reporting_year']:d}-{row['reporting_month']:02d}")

            if cumulative:
                y_axis.append(bytes_to_terabytes(cumulative_volume_bytes))
            else:
                y_axis.append(bytes_to_terabytes(stride_accumulator))

            stride_accumulator = 0

    # Handle any trailing partial stride window so the last month(s) aren't dropped
    if stride_accumulator > 0 and stride_count % stride_months != 0:
        last = rows[-1]
        x_axis.append(f"{last['reporting_year']:d}-{last['reporting_month']:02d}")
        if cumulative:
            y_axis.append(bytes_to_terabytes(cumulative_volume_bytes))
        else:
            y_axis.append(bytes_to_terabytes(stride_accumulator))

    volume_petabytes = bytes_to_petabytes(cumulative_volume_bytes)

    fig, _ = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title(f"{title} = {volume_petabytes:.3f} PB (as at {time.strftime('%d-%b-%Y')})")
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Terabytes (TB)")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")


def plot_downloads_per_month(
    monthly_data,
    date_from,
    date_to,
    title,
    cumulative,
    filename,
):
    """Plot downloads per month from pre-fetched data."""
    clear_plots()

    rows = filter_monthly_by_range(monthly_data, date_from, date_to)

    x_axis = []
    y_axis = []
    cumulative_downloaded_bytes = 0

    stride_months = 2 if (date_to - date_from).days > (6 * 31) else 1
    stride_accumulator = 0
    stride_count = 0

    for row in rows:
        downloaded_bytes = row["downloaded_bytes"]
        cumulative_downloaded_bytes += downloaded_bytes
        stride_accumulator += downloaded_bytes
        stride_count += 1

        # Check striding - step every N rows from start, not by calendar month parity
        if stride_count % stride_months == 0:
            x_axis.append(f"{row['reporting_year']:d}-{row['reporting_month']:02d}")

            if cumulative:
                y_axis.append(bytes_to_terabytes(cumulative_downloaded_bytes))
            else:
                y_axis.append(bytes_to_terabytes(stride_accumulator))

            stride_accumulator = 0

    # Handle any trailing partial stride window so the last month(s) aren't dropped
    if stride_accumulator > 0 and stride_count % stride_months != 0:
        last = rows[-1]
        x_axis.append(f"{last['reporting_year']:d}-{last['reporting_month']:02d}")
        if cumulative:
            y_axis.append(bytes_to_terabytes(cumulative_downloaded_bytes))
        else:
            y_axis.append(bytes_to_terabytes(stride_accumulator))

    volume_petabytes = bytes_to_petabytes(cumulative_downloaded_bytes)

    fig, _ = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title(f"{title} = {volume_petabytes:.3f} PB (as at {time.strftime('%d-%b-%Y')})")
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Terabytes (TB)")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")


def plot_archive_volume_per_project(project_data, title, filename):
    """Plot archive volume per project as a pie chart from pre-fetched data."""
    clear_plots()

    labels = []
    x_values = []
    max_slices = 20
    other_bytes = 0

    # Sort by archived bytes descending for correct pie slicing
    sorted_data = sorted(project_data, key=lambda r: r["total_archived_bytes"], reverse=True)

    for i, row in enumerate(sorted_data):
        value_bytes = row["total_archived_bytes"]

        if i >= max_slices:
            other_bytes += value_bytes
        else:
            x_values.append(bytes_to_terabytes(value_bytes))
            labels.append(f"{row['projectid']}-{row['projectshortname']}")

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
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")


def plot_telescope_time_per_project(project_data, title, filename):
    """Plot telescope time per project as a pie chart from pre-fetched data."""
    clear_plots()

    labels = []
    x_values = []
    max_slices = 8
    other_time = 0

    # Sort by hours descending so the top-N are the biggest time consumers
    sorted_data = sorted(project_data, key=lambda r: r["totaltime_hours"], reverse=True)

    for i, row in enumerate(sorted_data):
        if i >= max_slices:
            other_time += row["totaltime_hours"]
        else:
            labels.append(f"{row['projectid']}-{row['projectshortname']}")
            x_values.append(row["totaltime_hours"])

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
    logger.info(f"Saved {filename} to disk.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_stats(config_filename, no_pawsey: bool = False):
    """Main function"""
    # Usage: python stats.py --cfg=path/to/config/file

    config = ConfigParser()
    config.read(config_filename)

    minio_path = config.get("S3", "minio_path")
    if not os.path.exists(minio_path):
        print(f"Path to minio not valid: {minio_path}")
        sys.exit(1)

    mwa_db = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=2,
        host=config.get("MWA Database", "dbhost"),
        user=config.get("MWA Database", "dbuser"),
        database=config.get("MWA Database", "dbname"),
        password=config.get("MWA Database", "dbpass"),
        port=config.getint("MWA Database", "dbport"),
    )

    acacia_mwa_quota_bytes = config.getint("asvo_stats", "acacia_mwa_quota_bytes")
    banksia_quota_bytes = config.getint("asvo_stats", "banksia_quota_bytes")

    tap_url = config.get("MWA TAP", "url")
    mwa_tap_service = vo.dal.TAPService(tap_url)

    # Example of the expected format is: 01-Jul-2022
    # NOTE: keeping these in case I have a use for them later!
    _special_date_from = datetime.strptime(config.get("asvo_stats", "special_date_from"), "%d-%b-%Y").astimezone(AWST)
    _special_date_to = datetime.strptime(config.get("asvo_stats", "special_date_to"), "%d-%b-%Y").astimezone(AWST)

    today = datetime.now(tz=AWST)
    start_date = datetime(2006, 1, 1, tzinfo=AWST)

    six_months_ago = today - relativedelta(months=6)

    # --- Location summary (TAP) ---
    logger.info("Getting summary stats from database...")
    (
        db_acacia_mwa_bytes,
        db_banksia_bytes,
    ) = get_location_summary_bytes(mwa_tap_service)

    lts_percent_used = ((db_acacia_mwa_bytes + db_banksia_bytes) / (acacia_mwa_quota_bytes + banksia_quota_bytes)) * 100
    logger.info(
        "Pawsey Quota Used:"
        f" {bytes_to_terabytes(db_acacia_mwa_bytes + db_banksia_bytes):.3f} TB"
        f" / {bytes_to_terabytes(acacia_mwa_quota_bytes + banksia_quota_bytes):.3f} TB"
        f" == {lts_percent_used:.1f} % used"
    )

    # --- S3 stats (optional) ---
    if no_pawsey:
        logger.info("Skipping stats from Acacia_mwa and Banksia (--no-pawsey passed in)")
    else:
        if config.getint("S3", "use_acacia_mwa") == 1:
            acacia_mwa_profile = config.get("S3", "acacia_mwa_profile")
            acacia_mwa_endpoint_url = config.get("S3", "acacia_mwa_endpoint_url")

            logger.info("Getting stats from Acacia_mwa...")
            acacia_mwa_bytes = get_acacia_usage(acacia_mwa_profile, acacia_mwa_endpoint_url, minio_path)
        else:
            logger.info("Skipping stats from Acacia_mwa (use_acacia_mwa != 1)")
            acacia_mwa_bytes = 0

        if config.getint("S3", "use_banksia") == 1:
            banksia_profile = config.get("S3", "banksia_profile")
            banksia_endpoint_urls = str(config.get("S3", "banksia_endpoint_urls")).split(",")

            logger.info("Getting stats from Banksia...")
            banksia_bytes = get_banksia_usage(banksia_profile, banksia_endpoint_urls, minio_path)
        else:
            logger.info("Skipping stats from Banksia (use_banksia != 1)")
            banksia_bytes = 0

        logger.info(
            f"Acacia mwa       vs DB         : {bytes_to_terabytes(acacia_mwa_bytes)} TB vs"
            f" {bytes_to_terabytes(db_acacia_mwa_bytes)} TB"
        )
        logger.info(
            f"Banksia vs DB        : {bytes_to_terabytes(banksia_bytes)} TB vs"
            f" {bytes_to_terabytes(db_banksia_bytes)} TB"
        )
        logger.info("--------------------------------------------------")
        logger.info(
            "Total Pawsey LTS vs DB:"
            f" {bytes_to_terabytes(acacia_mwa_bytes + banksia_bytes)} TB vs"
            f" {bytes_to_terabytes(db_acacia_mwa_bytes + db_banksia_bytes)} TB"
        )
        logger.info("--------------------------------------------------")

        acacia_mwa_percent_used = (acacia_mwa_bytes / acacia_mwa_quota_bytes) * 100.0
        logger.info(
            "Acacia_mwa Quota Used:"
            f" {bytes_to_terabytes(acacia_mwa_bytes):.3f} TB /"
            f" {bytes_to_terabytes(acacia_mwa_quota_bytes):.3f} TB == "
            f" {acacia_mwa_percent_used:.1f} % used"
        )
        banksia_percent_used = (banksia_bytes / banksia_quota_bytes) * 100.0
        logger.info(
            "Banksia Quota Used:"
            f" {bytes_to_terabytes(banksia_bytes):.3f} TB /"
            f" {bytes_to_terabytes(banksia_quota_bytes):.3f} TB == "
            f" {banksia_percent_used:.1f} % used"
        )
        lts_percent_used = ((acacia_mwa_bytes + banksia_bytes) / (acacia_mwa_quota_bytes + banksia_quota_bytes)) * 100
        logger.info(
            "Pawsey Quota Used:"
            f" {bytes_to_terabytes(acacia_mwa_bytes + banksia_bytes):.3f} TB"
            f" / {bytes_to_terabytes(acacia_mwa_quota_bytes + banksia_quota_bytes):.3f} TB"
            f" == {lts_percent_used:.1f} % used"
        )

        logger.info("\n-------------------------------------------------------")

        acacia_mwa_available_bytes = acacia_mwa_quota_bytes - acacia_mwa_bytes
        banksia_available_bytes = banksia_quota_bytes - banksia_bytes
        pawsey_available_bytes = acacia_mwa_available_bytes + banksia_available_bytes

        logger.info(f"Acacia_mwa Quota Available: {bytes_to_terabytes(acacia_mwa_available_bytes):.3f} TB ")
        logger.info(f"Banksia Quota Available: {bytes_to_terabytes(banksia_available_bytes):.3f} TB ")
        logger.info(f"Pawsey Quota Available: {bytes_to_terabytes(pawsey_available_bytes):.3f} TB ")
    logger.info("-------------------------------------------------------\n")

    # ---------------------------------------------------------------
    # Fetch data once
    # ---------------------------------------------------------------
    monthly_data = fetch_monthly_data(mwa_tap_service)
    daily_data = fetch_daily_stats(mwa_tap_service)
    project_data_all = fetch_project_data(mwa_tap_service)
    project_data_6mo = fetch_project_data(mwa_tap_service, six_months_ago, today)
    deleted_data = fetch_deleted_data_by_month(mwa_db)
    deleted_lookup = build_deleted_lookup(deleted_data)

    logger.info(
        f"Fetched {len(monthly_data)} monthly rows, "
        f"{len(project_data_all)} projects (all time), "
        f"{len(project_data_6mo)} projects (6 mo), "
        f"{len(deleted_data)} deleted-data months."
    )

    # ---------------------------------------------------------------
    # CSV dumps (use pre-fetched data)
    # ---------------------------------------------------------------
    dump_stats(daily_data, "stats.csv")
    dump_monthly_stats(monthly_data, "stats_by_month.csv")
    dump_stats_by_project(project_data_all, "stats_by_project.csv")

    # ---------------------------------------------------------------
    # Plots (use pre-fetched data, filter in Python)
    # ---------------------------------------------------------------

    # Downloads
    plot_downloads_per_month(
        monthly_data,
        start_date,
        today,
        "Downloads per month (all time)",
        False,
        "mwa_downloads_per_month_all_time.png",
    )

    plot_downloads_per_month(
        monthly_data,
        start_date,
        today,
        "Cumulative downloads per month (all time)",
        True,
        "mwa_downloads_per_month_all_time_cuml.png",
    )

    # Archive volume — all time
    plot_archive_volume_per_month(
        monthly_data,
        deleted_lookup,
        start_date,
        today,
        "MWA Archive Volume (all time)",
        True,
        "mwa_archive_volume_all_time.png",
        False,
    )
    plot_archive_volume_per_month(
        monthly_data,
        deleted_lookup,
        start_date,
        today,
        "MWA Archive Ingest (all time)",
        True,
        "mwa_archive_ingest_all_time.png",
        True,
    )

    # Per-project — all time
    plot_archive_volume_per_project(
        project_data_all,
        "MWA Archive Volume by Project (all time)",
        "mwa_archive_volume_by_project_all_time.png",
    )
    plot_telescope_time_per_project(
        project_data_all,
        "MWA Telescope Time (all time)",
        "mwa_telescope_time_all_time.png",
    )

    # Archive volume — last 6 months
    plot_archive_volume_per_month(
        monthly_data,
        deleted_lookup,
        six_months_ago,
        today,
        "MWA Archive Net Growth (last 6 months)",
        False,
        "mwa_archive_net_growth_last_6_months.png",
        False,
    )
    plot_archive_volume_per_month(
        monthly_data,
        deleted_lookup,
        six_months_ago,
        today,
        "MWA Archive Ingest (last 6 months)",
        False,
        "mwa_archive_ingest_last_6_months.png",
        True,
    )

    # Per-project — last 6 months
    plot_archive_volume_per_project(
        project_data_6mo,
        "MWA Archive Volume by Project (last 6 months)",
        "mwa_archive_volume_by_project_last_6_months.png",
    )
    plot_telescope_time_per_project(
        project_data_6mo,
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
    parser.add_argument("-n", "--no-pawsey", action="store_true", help="Do not get Pawsey data sizes.\n")

    args = vars(parser.parse_args())

    run_stats(args["cfg"], args["no_pawsey"])
