# Python 3
from configparser import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import pyvo as vo
from numpy.core.defchararray import isdigit

DPI = 100


def do_query(vo_service, adql_statement):
    results = vo_service.search(adql_statement)
    return results


def dump_stats(vo_service, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("date", "projid", "config", "time(s)", "archived(bytes)",
                  "deleted(bytes)", "time(hours)", "archived(TB)")

        stats_csv_writer.writerow(header)

        total_bytes = 0.
        deleted_bytes = 0.
        total_secs = 0.

        results = do_query(vo_service, """SELECT 
                                                 date_trunc('day', starttime_utc) As reporting_date
                                                ,projectid
                                                ,mwa_array_configuration
                                                ,SUM(duration) as total_time_secs
                                                ,SUM(total_archived_data_bytes) as total_archived_data_bytes
                                                ,SUM(files_deleted_bytes) as deleted_bytes
                                            FROM mwa.observation
                                            GROUP BY 1,2,3
                                            ORDER BY 1,2""")

        for row in results:
            i = i + 1

            if not row['total_time_secs'] is None:
                total_secs += int(row['total_time_secs'])
                hours = int(row['total_time_secs']) / 3600
            else:
                hours = 0.

            if not row['total_archived_data_bytes'] is None:
                this_bytes = int(row['total_archived_data_bytes'])
                total_bytes += this_bytes
                terabytes = bytes_to_terabytes(this_bytes)
            else:
                terabytes = 0.

            if not row['deleted_bytes'] is None:
                deleted_bytes += int(row['deleted_bytes'])

            stats_csv_writer.writerow((row['reporting_date'],
                                       row['projectid'],
                                       row['mwa_array_configuration'],
                                       int(row['total_time_secs']),
                                       int(row['total_archived_data_bytes']),
                                       int(row['deleted_bytes']),
                                       hours,
                                       terabytes))

    print("{0} rows written to {1}.\n".format(i, filename))
    print(f"Total data: { bytes_to_petabytes(total_bytes) } PB\n")
    print(f"Total time: { total_secs / 3600 } hours\n")
    print(f"Total deleted data: { bytes_to_petabytes(deleted_bytes) } PB\n")

    # results = do_query(vo_service, """SELECT
    #                                        SUM(total_archived_data_bytes) as total_archived_bytes
    #                                       FROM mwa.observation
    #                                       WHERE dataqualityname = 'Marked for Delete'
    #                                   """)
    #
    # marked_for_delete_bytes = results[0]['total_archived_bytes']
    # if isdigit(marked_for_delete_bytes):
    #     marked_for_delete_bytes = int(marked_for_delete_bytes)
    # else:
    #     marked_for_delete_bytes = 0
    # print(f"Data marked for delete: { bytes_to_terabytes(marked_for_delete_bytes) } TB\n")


def dump_stats_by_project(local_db_conn, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("projid", "projname", "TB")

        stats_csv_writer.writerow(header)

        results = do_query(local_db_conn, """SELECT  
                                                  projectid
                                                 ,projectshortname                                                                                                     
                                                 ,SUM(total_archived_data_bytes) As total_archived_data_bytes
                                           FROM mwa.observation 
                                           GROUP BY projectid, 
                                                    projectshortname
                                           ORDER BY 3 DESC""")

        for row in results:
            i = i + 1

            # lookup project description
            projid = row['projectid']
            projname = row['projectshortname']

            terabytes = bytes_to_terabytes(int(row['total_archived_data_bytes']))
            stats_csv_writer.writerow((projid, projname, terabytes, ))

    print("{0} rows written to {1}.\n".format(i, filename))


def dump_monthly_stats(vo_service, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("year", "month", "hrs", "TB", "avail_hrs", "duty_cycle")
        stats_csv_writer.writerow(header)

        results = do_query(vo_service, """SELECT 
                                               date_part('year', date_trunc('day', starttime_utc)) as reporting_year
                                              ,date_part('month', date_trunc('day', starttime_utc)) as reporting_month
                                              ,SUM(duration) as month_secs
                                              ,SUM(total_archived_data_bytes) as month_bytes
                                          FROM mwa.observation
                                          GROUP BY 1,2
                                          ORDER BY 1,2""")

        for row in results:
            i = i + 1
            year = int(row['reporting_year'])
            month = int(row['reporting_month'])
            hours = row['month_secs']/3600
            data_bytes = row['month_bytes']

            terabytes = bytes_to_terabytes(data_bytes)
            available_hours = get_available_hours(year, month)
            duty_cycle = get_duty_cycle(hours, available_hours)

            csv_row = (year, month, hours, terabytes, available_hours, duty_cycle)

            stats_csv_writer.writerow(csv_row)

        print("{0} rows written to {1}.\n".format(i, filename))


def get_filetype_by_id(filetype_id):
    types = ["Unknown (0)",
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
             "Voltage Recombined TAR"]
    try:
        return types[filetype_id]
    except IndexError:
        return f"Unknown ({filetype_id})"


def get_duty_cycle(hours, available_hours):
    if available_hours > 0:
        return hours / available_hours
    else:
        return 0


def get_available_hours(year: int, month: int):
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
    f = plt.figure()

    if f:
        f.clear()
        plt.close(f)


def get_deleted_data_by_month(tap_service, date_from, date_to):

    results = do_query(tap_service, f"""SELECT 
                                             date_part('year', date_trunc('day', modtime)) as reporting_year 
                                            ,date_part('month', date_trunc('day', modtime)) as reporting_month                                             
                                            ,SUM(deleted_size) as deleted_bytes
                                        FROM mwa.obs_data_file
                                        WHERE    
                                             modtime BETWEEN '{date_from}' AND '{date_to}'
                                        GROUP BY 1,2
                                        ORDER BY 1,2""")

    return results


def do_plot_archive_volume_per_month(tap_service, date_from, date_to, title, cumulative, filename, ingest_only):
    clear_plots()

    x_axis = []
    y_axis = []
    cumulative_volume_bytes = 0

    # If showing more than 6 months, make the stride longer
    if (date_to - date_from).days > (6*31):
        stride_months = 3
    else:
        stride_months = 1

    results = do_query(tap_service, f"""SELECT 
                                              date_part('year', date_trunc('day', starttime_utc)) as reporting_year 
                                             ,date_part('month', date_trunc('day', starttime_utc)) as reporting_month
                                             ,SUM(total_archived_data_bytes + files_deleted_bytes) as total_data_bytes 
                                         FROM mwa.observation  
                                         WHERE    
                                             starttime_utc BETWEEN '{date_from}' AND '{date_to}'                                     
                                         GROUP BY 1,2
                                         ORDER BY 1,2 """)
    deleted_results = None
    if not ingest_only:
        deleted_results = get_deleted_data_by_month(tap_service, date_from, date_to)

    for row in results:
        this_bytes = int(row['total_data_bytes'])
        this_deleted_bytes = 0
        cumulative_volume_bytes += this_bytes

        if not ingest_only:
            # find row in deleted_results
            for drow in deleted_results:
                if row['reporting_year'] == drow['reporting_year'] and row['reporting_month'] == drow['reporting_month']:
                    deleted_bytes = int(drow['deleted_bytes'])
                    this_bytes -= deleted_bytes
                    this_deleted_bytes = deleted_bytes
                    cumulative_volume_bytes -= deleted_bytes

        volume_bytes = this_bytes

        # Check striding
        if row['reporting_month'] % stride_months == 0:
            x_axis.append("{0:d}-{1:02d}".format(int(row['reporting_year']), int(row['reporting_month'])))

            if cumulative:
                y_axis.append(bytes_to_terabytes(cumulative_volume_bytes))
            else:
                y_axis.append(bytes_to_terabytes(volume_bytes))

        # Only dump this debug to the screen if we are including deleted data and in the year and qtrs we want
        # and only if this code is being run on the full archive and not just 6 months worth
        # This dump code is here because it is convenient - it should be moved into a seperate module really
        dump_year_from = 2020
        dump_year_to = 2020
        dump_month_from = 9
        dump_month_to = 12

        if not ingest_only and \
            (date_to - date_from).days > (31*6) and \
            row['reporting_year'] >= dump_year_from and \
            row['reporting_year'] <= dump_year_to and \
            row['reporting_month'] >= dump_month_from and \
            row['reporting_month'] <= dump_month_to:

            print(row['reporting_year'],
                  row['reporting_month'],
                  bytes_to_terabytes(volume_bytes),
                  bytes_to_terabytes(volume_bytes + this_deleted_bytes),
                  bytes_to_terabytes(this_deleted_bytes),
                  bytes_to_terabytes(cumulative_volume_bytes))

    volume_petabytes = bytes_to_petabytes(cumulative_volume_bytes)

    fig, axis = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title("{0} = {1:.2f} PB (as at {2})".format(title, volume_petabytes, time.strftime("%d-%b-%Y")))
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Terabytes (TB)")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)


def do_plot_archive_volume_per_project(tap_service, date_from, date_to, title, filename):
    clear_plots()

    labels = []
    x_values = []
    slice_no = 0
    max_slices = 11
    other_bytes = 0

    results = do_query(tap_service, f"""SELECT projectid,
                                                projectshortname, 
                                                COALESCE(SUM(total_archived_data_bytes),0) as total_archived_data_bytes 
                                         FROM mwa.observation
                                         WHERE 
                                             starttime_utc BETWEEN '{date_from}' AND '{date_to}'  
                                         GROUP BY projectid,
                                                  projectshortname
                                         ORDER BY 3 DESC""")

    for row in results:
        value_bytes = int(row['total_archived_data_bytes'])

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
    axis.pie(x_values, labels=labels, autopct=lambda pct: pie_volume_format(pct, x_values), startangle=0)
    axis.axis('equal')

    plt.title("{0} (as at {1})".format(title, time.strftime("%d-%b-%Y")))
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI, )


def do_plot_telescope_time_per_project(tap_service, date_from, date_to, title, filename):
    clear_plots()

    labels = []
    x_values = []
    slice_no = 0
    max_slices = 8
    other_time = 0

    results = do_query(tap_service, f"""SELECT projectid,
                                                projectshortname,
                                                COALESCE(SUM(duration),0)/3600 As totaltime_hours
                                         FROM mwa.observation
                                         WHERE 
                                             starttime_utc BETWEEN '{date_from}' AND '{date_to}'  
                                         GROUP BY projectid,
                                                  projectshortname
                                         ORDER BY 3 DESC""")

    for row in results:
        if slice_no >= max_slices:
            other_time += int(row['totaltime_hours'])
        else:
            labels.append(f"{row['projectid']}-{row['projectshortname']}")
            x_values.append(int(row['totaltime_hours']))

        slice_no += 1

    # Add 'Other'
    labels.append("Other")
    x_values.append(other_time)

    fig, axis = plt.subplots()
    axis.pie(x_values, labels=labels, autopct=lambda pct: pie_hours_format(pct, x_values), startangle=0)
    axis.axis('equal')
    plt.title("{0} by Project (as at {1})".format(title, time.strftime("%d-%b-%Y")))
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)


def pie_hours_format(pct, allvals):
    absolute = int(pct/100. * float(np.sum(allvals)))

    if pct < 5:
        return "{:.1f}%".format(pct, absolute)
    else:
        return "{:.1f}%\n({:d} hrs)".format(pct, absolute)


def pie_volume_format(pct, allvals):
    absolute = int(pct/100. * float(np.sum(allvals)))

    if pct < 5:
        return "{:.1f}%".format(pct, absolute)
    else:
        return "{:.1f}%\n({:d} TB)".format(pct, absolute)


def bytes_to_terabytes(bytes_value):
    if bytes_value is None:
        return 0.
    else:
        return float(bytes_value) / (1000. * 1000. * 1000. * 1000.)


def bytes_to_petabytes(bytes_value):
    if bytes_value is None:
        return 0.
    else:
        return float(bytes_value) / (1000. * 1000. * 1000. * 1000. * 1000.)


def run_stats():
    #
    # Usage: python stats.py
    #
    app_path = os.path.dirname(os.path.realpath(__file__))
    config = ConfigParser()
    config.read(app_path + '/' + 'config.cfg')

    tap_url = config.get("MWA TAP", "url")
    mwa_tap_service = vo.dal.TAPService(tap_url)

    today = datetime.today()
    start_date = datetime(2006, 1, 1)

    six_months_ago = today - relativedelta(months=6)

    # Either way show whats in the db
    dump_stats(mwa_tap_service, "stats.csv")
    dump_monthly_stats(mwa_tap_service, "stats_by_month.csv")
    dump_stats_by_project(mwa_tap_service, "stats_by_project.csv")

    do_plot_archive_volume_per_month(mwa_tap_service, start_date, today,
                                     "MWA Archive Volume (all time)", True,
                                     "mwa_archive_volume_all_time.png", False)
    do_plot_archive_volume_per_month(mwa_tap_service, start_date, today,
                                     "MWA Archive Ingest (all time)", True,
                                     "mwa_archive_ingest_all_time.png", True)
    do_plot_archive_volume_per_project(mwa_tap_service, start_date, today,
                                       "MWA Archive Volume by Project (all time)",
                                       "mwa_archive_volume_by_project_all_time.png")
    do_plot_telescope_time_per_project(mwa_tap_service, start_date, today,
                                       "MWA Telescope Time (all time)",
                                       "mwa_telescope_time_all_time.png")

    do_plot_archive_volume_per_month(mwa_tap_service, six_months_ago, today,
                                     "MWA Archive Net Growth (last 6 months)", False,
                                     "mwa_archive_net_growth_last_6_months.png", False)
    do_plot_archive_volume_per_month(mwa_tap_service, six_months_ago, today,
                                     "MWA Archive Ingest (last 6 months)", False,
                                     "mwa_archive_ingest_last_6_months.png", True)
    do_plot_archive_volume_per_project(mwa_tap_service, six_months_ago, today,
                                       "MWA Archive Volume by Project (last 6 months)",
                                       "mwa_archive_volume_by_project_last_6_months.png")
    do_plot_telescope_time_per_project(mwa_tap_service, six_months_ago, today,
                                       "MWA Telescope Time (last 6 months)",
                                       "mwa_telescope_time_last_6_months.png")


if __name__ == "__main__":
    run_stats()
