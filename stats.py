# Python 3
import traceback
import base64
from configparser import ConfigParser
import numpy as np
import matplotlib.pyplot as plt
import os
import calendar
import psycopg2.pool
from psycopg2.extras import execute_values
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import warnings
from astropy.time import Time
warnings.filterwarnings('ignore')

DPI = 100


def dump_stats(local_db_conn, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("date", "projid", "config", "secs", "bytes", "hours", "TB")

        stats_csv_writer.writerow(header)

        total_bytes = 0
        deleted_bytes = 0
        total_secs = 0

        conn = None
        try:
            conn = local_db_conn.getconn()
            cursor = conn.cursor()

            cursor.execute("""SELECT 
                                 date_trunc('day', starttime_utc) As reporting_date
                                ,projectid
                                ,mwa_array_configuration
                                ,SUM(duration) as total_time_secs
                                ,SUM(total_archived_data_bytes) as total_archived_data_bytes
                                ,SUM(files_deleted_bytes) as deleted_bytes
                            FROM mwa.observation_vw
                            GROUP BY 1,2,3
                            ORDER BY 1,2""")

            for row in cursor.fetchall():
                i = i + 1

                if not row[3] is None:
                    total_secs += int(row[3])
                    hours = row[3] / 3600
                else:
                    hours = 0

                if not row[4] is None:
                    total_bytes += int(row[4])
                    terabytes = bytes_to_terabytes(row[4])
                else:
                    terabytes = 0

                if not row[5] is None:
                    deleted_bytes += int(row[5])

                stats_csv_writer.writerow(row + (hours, terabytes))

        except (Exception, psycopg2.DatabaseError) as error:
            print(error, traceback.print_exc())

        finally:
            if conn is not None:
                local_db_conn.putconn(conn)

    print("{0} rows written to {1}.\n".format(i, filename))
    print(f"Total data: { bytes_to_petabytes(total_bytes) } PB\n")
    print(f"Total time: { total_secs / 3600 } hours\n")
    print(f"Total deleted data: { bytes_to_petabytes(deleted_bytes) } PB\n")

    marked_for_delete_bytes = 0

    conn = None
    try:
        conn = local_db_conn.getconn()
        cursor = conn.cursor()

        cursor.execute("""SELECT 
                           SUM(total_archived_data_bytes)   
                          FROM mwa.observation_vw
                          WHERE dataqualityname = 'Marked for Delete'                        
                        """)

        marked_for_delete_bytes = cursor.fetchone()[0]

    except (Exception, psycopg2.DatabaseError) as error:
        print(error, traceback.print_exc())

    finally:
        if conn is not None:
            local_db_conn.putconn(conn)

    print(f"Data marked for delete: { bytes_to_terabytes(marked_for_delete_bytes) } TB\n")

def dump_stats_by_project(local_db_conn, filename, project_names_list):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("projid", "projname", "TB")

        stats_csv_writer.writerow(header)

        total_bytes = 0

        conn = None
        try:
            conn = local_db_conn.getconn()
            cursor = conn.cursor()

            cursor.execute("""SELECT  
                                  projectid                                                                                                     
                                 ,SUM(total_archived_data_bytes) As total_archived_data_bytes
                           FROM mwa.observation_vw 
                           GROUP BY projectid
                           ORDER BY SUM(total_archived_data_bytes) DESC""")

            for row in cursor.fetchall():
                i = i + 1

                # lookup project description
                projid = row[0]
                projname = get_project_shortname_by_id(projid, project_names_list)

                terabytes = int(bytes_to_terabytes(row[1]))
                stats_csv_writer.writerow((projid, projname, terabytes, ))

        except (Exception, psycopg2.DatabaseError) as error:
            print(error, traceback.print_exc())

        finally:
            if conn is not None:
                local_db_conn.putconn(conn)

    print("{0} rows written to {1}.\n".format(i, filename))


def dump_monthly_stats(local_db_conn, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        conn = None
        try:
            conn = local_db_conn.getconn()
            cursor = conn.cursor()

            header = ("year", "month", "hrs", "TB", "avail_hrs", "duty_cycle")
            stats_csv_writer.writerow(header)

            cursor.execute("""SELECT 
                                   date_part('year', date_trunc('day', starttime_utc))::numeric::integer as Reporting_Year 
                                  ,date_part('month', date_trunc('day', starttime_utc))::numeric::integer as Reporting_Month	 	
                                  ,SUM(duration) as Month_secs
                                  ,SUM(total_archived_data_bytes) as Month_bytes
                              FROM mwa.observation_vw
                              GROUP BY 1,2
                              ORDER BY 1,2""")

            for row in cursor.fetchall():
                i = i + 1
                year = row[0]
                month = row[1]
                hours = row[2]/3600
                data_bytes = row[3]

                terabytes = bytes_to_terabytes(data_bytes)
                available_hours = get_available_hours(year, month)
                duty_cycle = get_duty_cycle(hours, available_hours)

                csv_row = (year, month, hours, terabytes, available_hours, duty_cycle)

                stats_csv_writer.writerow(csv_row)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error, traceback.print_exc())

        finally:
            if conn is not None:
                local_db_conn.putconn(conn)

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


def get_project_shortname_by_id(projectid, project_names_list):
    for p in project_names_list:
        if p[0] == projectid:
            # Also do some replacement/fixes for long names
            if p[0] == "G0021":
                return "Low-freq investigations of Parkes pta MSP"
            else:
                return p[1]

    return f"{projectid} Not found"


def get_duty_cycle(hours, available_hours):
    if available_hours > 0:
        return hours / available_hours
    else:
        return 0


def get_available_hours(year, month):
    start_date = datetime(year, month, 1)

    # get end date.
    month = month + 1
    if month == 13:
        month = 1
        year = year + 1

    end_date = datetime(year, month, 1)

    # return hours
    return (end_date - start_date).total_seconds() / 3600


def get_project_names(context):
    cursor = None
    con = None
    sql = """SELECT projectid, shortname FROM mwa_project ORDER BY projectid"""

    try:
        con = context.getconn()

        cursor = con.cursor()

        cursor.execute(sql)

        results = cursor.fetchall()
        return results

    finally:
        if cursor:
            cursor.close()

        if con:
            context.putconn(conn=con)


def clear_plots():
    f = plt.figure()

    if f:
        f.clear()
        plt.close(f)


def do_plot_archive_volume_per_month(local_db_conn, date_from, date_to, title, cumulative, filename):
    clear_plots()

    con = None
    cursor = None

    x_axis = []
    y_axis = []
    volume_bytes = 0
    cumulative_volume_bytes = 0

    # If showing more than 6 months, make the stride longer
    if (date_to - date_from).days > (6*31):
        stride_months = 3
    else:
        stride_months = 1

    sql = """SELECT 
                  date_part('year', date_trunc('day', starttime_utc))::numeric::integer as Reporting_Year 
                 ,date_part('month', date_trunc('day', starttime_utc))::numeric::integer as Reporting_Month
                 ,SUM(total_archived_data_bytes)
             FROM mwa.observation_vw  
             WHERE    
                 starttime_utc BETWEEN %s AND %s                                      
             GROUP BY 1,2
             ORDER BY 1,2 """

    try:
        con = local_db_conn.getconn()

        cursor = con.cursor()

        cursor.execute(sql, (date_from, date_to))

        results = cursor.fetchall()

        for row in results:
            cumulative_volume_bytes += row[2]
            volume_bytes = row[2]

            # Check striding
            if row[1] % stride_months == 0:
                x_axis.append("{0:d}-{1:02d}".format(int(row[0]), int(row[1])))

                if cumulative:
                    y_axis.append(bytes_to_terabytes(cumulative_volume_bytes))
                else:
                    y_axis.append(bytes_to_terabytes(volume_bytes))

    finally:
        if cursor:
            cursor.close()

        if con:
            local_db_conn.putconn(conn=con)

    volume_petabytes = bytes_to_petabytes(cumulative_volume_bytes)

    fig, axis = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title("{0} = {1:.2f} PB (as at {2})".format(title, volume_petabytes, time.strftime("%d-%b-%Y")))
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Terabytes (TB)")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)


def do_plot_archive_volume_per_project(local_db_conn, date_from, date_to, title, project_names_list, filename):
    clear_plots()

    con = None
    cursor = None

    labels = []
    x_values = []
    legend_labels = []
    slice_no = 0
    max_slices = 11
    other_bytes = 0

    sql = """SELECT projectid, COALESCE(SUM(total_archived_data_bytes),0) as total_archived_data_bytes 
             FROM mwa.observation_vw
             WHERE 
                 starttime_utc BETWEEN %s AND %s  
             GROUP BY projectid
             ORDER BY COALESCE(SUM(total_archived_data_bytes),0) DESC"""

    try:
        con = local_db_conn.getconn()

        cursor = con.cursor()

        cursor.execute(sql, (date_from, date_to))

        results = cursor.fetchall()

        for row in results:
            value_bytes = row[1]

            if slice_no >= max_slices:
                other_bytes += value_bytes
            else:
                x_values.append(bytes_to_terabytes(value_bytes))
                labels.append(f"{row[0]}-{get_project_shortname_by_id(row[0], project_names_list)}")

            slice_no += 1

    finally:
        if cursor:
            cursor.close()

        if con:
            local_db_conn.putconn(conn=con)

    # Add 'Other'
    labels.append("Other")
    x_values.append(bytes_to_terabytes(other_bytes))

    fig, axis = plt.subplots()
    axis.pie(x_values, labels=labels, autopct=lambda pct: pie_volume_format(pct, x_values), startangle=0)
    axis.axis('equal')

    plt.title("{0} (as at {1})".format(title, time.strftime("%d-%b-%Y")))
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI, )


def do_plot_telescope_time_per_project(local_db_conn, date_from, date_to, title, project_names_list, filename):
    clear_plots()

    con = None
    cursor = None

    labels = []
    x_values = []
    slice_no = 0
    max_slices = 8
    other_time = 0

    sql = """SELECT projectid, CAST(COALESCE(SUM(duration),0)/3600 As Integer) As TotalTime_hours
             FROM mwa.observation_vw
             WHERE 
                 starttime_utc BETWEEN %s AND %s  
             GROUP BY projectid
             ORDER BY CAST(COALESCE(SUM(duration),0)/3600 As Integer) DESC"""

    try:
        con = local_db_conn.getconn()

        cursor = con.cursor()

        cursor.execute(sql, (date_from, date_to))

        results = cursor.fetchall()

        for row in results:
            if slice_no >= max_slices:
                other_time += row[1]
            else:
                labels.append(f"{row[0]}-{get_project_shortname_by_id(row[0], project_names_list)}")
                x_values.append(row[1])

            slice_no += 1

    finally:
        if cursor:
            cursor.close()

        if con:
            local_db_conn.putconn(conn=con)

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
        return 0
    else:
        return bytes_value / (1000 * 1000 * 1000 * 1000)


def bytes_to_petabytes(bytes_value):
    if bytes_value is None:
        return 0
    else:
        return bytes_value / (1000 * 1000 * 1000 * 1000 * 1000)


def run_stats():
    #
    # Usage: python stats.py
    #
    app_path = os.path.dirname(os.path.realpath(__file__))
    config = ConfigParser()
    config.read(app_path + '/' + 'config.cfg')

    mwa_context = psycopg2.pool.ThreadedConnectionPool(minconn=1,
                                                       maxconn=1,
                                                       host=config.get("MWA Database", "dbhost"),
                                                       user=config.get("MWA Database", "dbuser"),
                                                       database=config.get("MWA Database", "dbname"),
                                                       password=base64.b64decode(
                                                           config.get("MWA Database", "dbpass")).decode("utf-8"),
                                                       port=5432)

    local_db_context = psycopg2.pool.ThreadedConnectionPool(minconn=1,
                                                            maxconn=1,
                                                            host=config.get("Stats Database", "dbhost"),
                                                            user=config.get("Stats Database", "dbuser"),
                                                            database=config.get("Stats Database", "dbname"),
                                                            password=base64.b64decode(
                                                              config.get("Stats Database", "dbpass")).decode("utf-8"),
                                                            port=5432)

    today = datetime.today()
    start_date = datetime(2006, 1, 1)

    six_months_ago = today - relativedelta(months=6)

    # Load project name
    project_names_list = get_project_names(mwa_context)

    # Either way show whats in the db
    dump_stats(local_db_context, "stats.csv")
    dump_monthly_stats(local_db_context, "stats_by_month.csv")
    dump_stats_by_project(local_db_context, "stats_by_project.csv", project_names_list)

    do_plot_archive_volume_per_month(local_db_context, start_date, today,
                                     "MWA Archive Volume (all time)", True,
                                     "mwa_archive_volume_all_time.png")
    do_plot_archive_volume_per_project(local_db_context, start_date, today,
                                       "MWA Archive Volume by Project (all time)", project_names_list,
                                       "mwa_archive_volume_by_project_all_time.png")
    do_plot_telescope_time_per_project(local_db_context, start_date, today,
                                       "MWA Telescope Time (all time)", project_names_list,
                                       "mwa_telescope_time_all_time.png")

    do_plot_archive_volume_per_month(local_db_context, six_months_ago, today,
                                     "MWA Archive Net Growth (last 6 months)", False,
                                     "mwa_archive_net_growth_last_6_months.png")
    do_plot_archive_volume_per_project(local_db_context, six_months_ago, today,
                                       "MWA Archive Volume by Project (last 6 months)", project_names_list,
                                       "mwa_archive_volume_by_project_last_6_months.png")
    do_plot_telescope_time_per_project(local_db_context, six_months_ago, today,
                                       "MWA Telescope Time (last 6 months)", project_names_list,
                                       "mwa_telescope_time_last_6_months.png")


if __name__ == "__main__":
    run_stats()
