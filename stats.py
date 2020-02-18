# Python 3
import traceback
import argparse
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

CONST_PHASE_0 = "Commissioning"
CONST_PHASE_I = "Phase I"
CONST_PHASE_IIA = "Phase IIA" # Compact mode
CONST_PHASE_IIB = "Phase IIB" # Long baseline / extended mode
DPI = 100


def get_array_config(in_date):
    if in_date < datetime(2013, 7, 1):
        return CONST_PHASE_0
    elif in_date < datetime(2016, 8, 9):
        return CONST_PHASE_I
    elif in_date < datetime(2017, 9, 19):
        return CONST_PHASE_IIA
    elif in_date < datetime(2018, 8, 29):
        return CONST_PHASE_IIB
    elif in_date < datetime(2019, 1, 17):
        return CONST_PHASE_IIA
    elif in_date < datetime(2019, 8, 19):
        return CONST_PHASE_IIB
    elif in_date >= datetime(2019, 8, 19):
        return CONST_PHASE_IIA


def dump_stats(local_db_conn, filename):
    i = 0

    with open(filename, mode='w') as stats_csv_file:
        stats_csv_writer = csv.writer(stats_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        header = ("date", "projid", "filetype", "config", "secs", "bytes", "hours", "TB")

        stats_csv_writer.writerow(header)

        total_bytes = 0
        total_secs = 0

        conn = None
        try:
            conn = local_db_conn.getconn()
            cursor = conn.cursor()

            cursor.execute("""SELECT Reporting_date 
                                 ,projid 
                                 ,filetype 
                                 ,array_configuration 
                                 ,TotalTime_secs
                                 ,TotalData_bytes 
                           FROM mwa_volume 
                           ORDER BY Reporting_date, projid, filetype""")

            for row in cursor.fetchall():
                i = i + 1

                if not row[4] is None:
                    total_secs += int(row[4])
                    hours = row[4] / 3600
                else:
                    hours = 0

                if not row[5] is None:
                    total_bytes += int(row[5])
                    terabytes = bytes_to_terabytes(row[5])
                else:
                    terabytes = 0

                stats_csv_writer.writerow(row + (hours, terabytes))

        except (Exception, psycopg2.DatabaseError) as error:
            print(error, traceback.print_exc())

        finally:
            if conn is not None:
                local_db_conn.putconn(conn)

    print("{0} rows written to {1}.\n".format(i, filename))
    print(f"Total data: { bytes_to_petabytes(total_bytes) } PB\n")
    print(f"Total time: { total_secs / 3600 } hours\n")


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
                                  projid                                                                    
                                 ,SUM(TotalData_bytes) As sum_of_totaldata_bytes
                           FROM mwa_volume 
                           GROUP BY projid
                           ORDER BY SUM(TotalData_bytes) DESC""")

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

            header = ("year", "month", "hrs", "TB", "avail_hrs", "duty_cycle", "config")
            stats_csv_writer.writerow(header)

            cursor.execute("""SELECT 
                                   date_part('year', reporting_date)::numeric::integer as Reporting_Year 
                                  ,date_part('month', reporting_date)::numeric::integer as Reporting_Month 
                                  ,SUM(TotalTime_secs) as Month_secs 
                                  ,SUM(TotalData_bytes) as Month_bytes 
                            FROM mwa_volume 
                            GROUP BY Reporting_Year, Reporting_Month 
                            ORDER BY Reporting_Year, Reporting_Month """)

            for row in cursor.fetchall():
                i = i + 1
                year = row[0]
                month = row[1]
                hours = row[2]/3600
                data_bytes = row[3]

                terabytes = bytes_to_terabytes(data_bytes)
                available_hours = get_available_hours(year, month)
                array_config = get_array_config(datetime(year, month, 1))
                duty_cycle = get_duty_cycle(hours, available_hours)

                csv_row = (year, month, hours, terabytes, available_hours, duty_cycle, array_config)

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


def insert_stats_into_local_db(mwa_context, from_date, to_date, local_db_connection):
    ft = Time(from_date, scale='utc')
    tt = Time(to_date, scale='utc')

    gps_start = ft.gps
    gps_end = tt.gps

    results = run_query(mwa_context, gps_start, gps_end)

    insert_list = []

    for r in results:
        report_date = datetime.combine(r[0], datetime.min.time())
        projid = r[1]

        if r[2] is None:
            ftype = None
        else:
            ftype = get_filetype_by_id(int(r[2]))

        aconfig = get_array_config(report_date)

        if r[3] is None:
            secs = None
        else:
            secs = int(r[3])

        if r[4] is None:
            bytes = None
        else:
            bytes = int(r[4])

        # Reporting_date, project id, file type, array_configuration, TotalTime_secs, TotalData_bytes
        insert_tuple = (report_date, projid, ftype, aconfig, secs, bytes)
        insert_list.append(insert_tuple)

    conn = None
    try:
        conn = local_db_connection.getconn()
        cursor = conn.cursor()

        execute_values(cursor, """INSERT INTO mwa_volume 
                                  (
                                   reporting_date
                                  ,projid
                                  ,filetype
                                  ,array_configuration
                                  ,totaltime_secs
                                  ,totaldata_bytes
                                  ) VALUES %s""", insert_list)

        cursor.close()

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            local_db_connection.putconn(conn)


def run_query(context, gps_starttime, gps_endtime):
    cursor = None
    con = None

    sql = """    
        SELECT 
             public.timestamp_gps(a.starttime)::timestamp::date as Reporting_date 
            ,p.projectid AS projid
            ,b.filetype
            ,NULL AS totaltime_sec
            ,sum(size) AS TotalData_bytes 
        FROM mwa_setting AS a 
        INNER JOIN mwa_project AS p ON p.projectid = a.projectid 
        INNER JOIN data_files AS b on a.starttime = b.observation_num 
        WHERE 
            a.mode IN ('HW_LFILES', 'VOLTAGE_START', 'VOLTAGE_BUFFER') 
            AND a.starttime BETWEEN %s AND %s 
            AND b.deleted = False AND b.remote_archived = True
            AND a.dataquality IN (1,2,3,6) --Good, some issues, unusable and processed 
        GROUP BY 
             public.timestamp_gps(a.starttime)::timestamp::date
            ,p.projectid
            ,b.filetype 
        UNION
        SELECT 
             public.timestamp_gps(a.starttime)::timestamp::date as Reporting_date 
            ,p.projectid AS projid
            ,NULL As filetype
            ,SUM(a.stoptime-a.starttime) AS totaltime_sec
            ,NULL AS totalsize_bytes 
        FROM mwa_setting AS a 
        INNER JOIN mwa_project AS p ON p.projectid = a.projectid 
        WHERE 
            a.mode IN ('HW_LFILES', 'VOLTAGE_START', 'VOLTAGE_BUFFER') 
            AND a.starttime BETWEEN %s AND %s
            AND a.dataquality IN (1,2,4,6) --Good, some issues, deleted and processed  
        GROUP BY             
             public.timestamp_gps(a.starttime)::timestamp::date
            ,p.projectid                                   
    """

    try:
        con = context.getconn()

        cursor = con.cursor()

        cursor.execute(sql, (gps_starttime, gps_endtime, gps_starttime, gps_endtime))

        results = cursor.fetchall()

        return results

    finally:
        if cursor:
            cursor.close()

        if con:
            context.putconn(conn=con)


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


def create_tables(local_db_conn):

    # Create table
    conn = None
    try:
        conn = local_db_conn.getconn()
        cursor = conn.cursor()

        cursor.execute('''DROP TABLE IF EXISTS mwa_volume;                          
        
                         CREATE TABLE mwa_volume
                         (id SERIAL NOT NULL,
                          Reporting_date date NOT NULL, 
                          projid text NOT NULL,
                          filetype text NULL,
                          array_configuration text NOT NULL, 
                          TotalTime_secs INT NULL, 
                          TotalData_bytes BIGINT NULL,                      
                          PRIMARY KEY (id));
                          COMMIT;
                          
                          CREATE UNIQUE INDEX mwa_volume_uk1 ON mwa_volume  
                                      (Reporting_date, projid, filetype)
                                       WHERE filetype IS NOT NULL;
                                       
                          CREATE UNIQUE INDEX mwa_volume_uk2 ON mwa_volume  
                                      (Reporting_date, projid, COALESCE(filetype,'None'))
                                       WHERE filetype IS NULL;                                                                              
                          ''')

        cursor.close()

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit(1)
    finally:
        if conn is not None:
            local_db_conn.putconn(conn)


def clear_months(date_from, date_to, local_db_conn):
    cal = calendar.monthrange(date_to.year, date_to.month)
    last_day = cal[1]

    from_dt = datetime(date_from.year, date_from.month, 1, 0, 0, 0)
    to_dt = datetime(date_to.year, date_to.month, last_day, 23, 59, 59)

    print("Clearing {0} to {1}...".format(from_dt, to_dt))

    conn = None
    try:
        conn = local_db_conn.getconn()
        cursor = conn.cursor()

        # Clear the table
        cursor.execute("""DELETE 
                          FROM mwa_volume 
                          WHERE Reporting_date BETWEEN %s AND %s""", (from_dt, to_dt))

        cursor.close()

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            local_db_conn.putconn(conn)


def populate(date_from, date_to, local_db_conn, mwa_context):
    print(f"Populate: {date_from} to {date_to}...")

    this_date = date_from
    months = relativedelta(date_to, date_from).months + 1

    for m in range(0, months + 1):
        cal = calendar.monthrange(this_date.year, this_date.month)
        last_day = cal[1]

        from_dt = datetime(this_date.year, this_date.month, 1, 0, 0, 0)
        to_dt = datetime(this_date.year, this_date.month, last_day, 23, 59, 59)
        print("Working on: {0} to {1}...".format(from_dt, to_dt))
        insert_stats_into_local_db(mwa_context, from_dt, to_dt, local_db_conn)

        # Increment date by 1 month
        this_date = this_date + relativedelta(this_date, months=1)

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
                  DATE_PART('year', Reporting_date) As Reporting_Year
                 ,DATE_PART('month', Reporting_date) As Reporting_Month
                 ,SUM(TotalData_bytes) 
             FROM mwa_volume  
             WHERE    
                 Reporting_date BETWEEN %s AND %s                                      
             GROUP BY DATE_PART('year', Reporting_date), DATE_PART('month', Reporting_date)
             ORDER BY DATE_PART('year', Reporting_date), DATE_PART('month', Reporting_date) """

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
    max_slices = 9
    other_bytes = 0

    sql = """SELECT projid, COALESCE(SUM(TotalData_bytes),0) as TotalData_bytes 
             FROM mwa_volume
             WHERE 
                 Reporting_date BETWEEN %s AND %s  
             GROUP BY projid
             ORDER BY COALESCE(SUM(TotalData_bytes),0) DESC"""

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

    sql = """SELECT projid, CAST(COALESCE(SUM(TotalTime_secs),0)/3600 As Integer) As TotalTime_hours
             FROM mwa_volume
             WHERE 
                 Reporting_Date BETWEEN %s AND %s  
             GROUP BY projid
             ORDER BY CAST(COALESCE(SUM(TotalTime_secs),0)/3600 As Integer) DESC"""

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
    parser = argparse.ArgumentParser(description='Get MWA Archive Stats.')
    parser.add_argument('--full-populate-db', action="store_true", dest="fullpopulatedb",
                        help='Clears and repopulates the sqlite db')
    parser.add_argument('--populate-replace', action="store_true", dest="populatereplace",
                        help='Clears and repopulates specific time periods in the sqlite db')
    parser.add_argument('--do-plots', action="store_true", dest="doplots",
                        help='Should the script create and show the plots? Default No')

    args = parser.parse_args()

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
    start_date = datetime(2010, 1, 1)

    six_months_ago = today - relativedelta(months=6)

    if args.fullpopulatedb:
        create_tables(local_db_context)

        for year in range(start_date.year, today.year+1):
            for month in range(1, 13):
                if datetime(year, month, 1) <= datetime.today():
                    populate(datetime(year, month, 1),
                             datetime(year, month, calendar.monthrange(year, month)[1]),
                             local_db_context, mwa_context)

    if args.populatereplace:
        clear_months(six_months_ago, datetime(today.year, today.month, 1),
                     local_db_context)
        populate(six_months_ago, datetime(today.year, today.month, 1),
                 local_db_context, mwa_context)

    # Load project name
    project_names_list = get_project_names(mwa_context)

    # Either way show whats in the db
    dump_stats(local_db_context, "stats.csv")
    dump_monthly_stats(local_db_context, "stats_by_month.csv")
    dump_stats_by_project(local_db_context, "stats_by_project.csv", project_names_list)

    if args.doplots:
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
