import argparse
import sqlite3
from datetime import datetime
from contextlib import closing
import numpy as np
import matplotlib.pyplot as plt


def do_plot_histogram(local_db_conn, date_from, date_to, title, min_seconds=None, max_seconds=None):
    x = []

    if min_seconds is None:
        min_seconds = 0

    if max_seconds is None:
        max_seconds = 999999999

    if max_seconds - min_seconds <= 3600:
        nbins = int(max_seconds - min_seconds)+1
    else:
        nbins = 100

    with closing(local_db_conn.cursor()) as c:
        for row in c.execute("""SELECT stage_seconds 
                                FROM mwa_staging
                                WHERE    
                                    stage_date_time BETWEEN ? AND ?                                
                                ORDER BY stage_date_time""",
                             (date_from, date_to)):
            value = row[0]

            if value >= min_seconds and value <= max_seconds:
                x.append(row[0])

    plt.hist(x, bins=nbins, normed=True)
    plt.title(f"{title} = {date_from} - {date_to}")
    plt.xlabel("Stage time (seconds)")
    plt.ylabel("Count")
    plt.show()


def create_table(local_db_conn):
    # Create table
    with closing(local_db_conn.cursor()) as c:
        c.execute('''CREATE TABLE IF NOT EXISTS mwa_staging
                     (stage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                      stage_date_time datetime NOT NULL,                      
                      stage_seconds integer NOT NULL);''')

        # Clear the table
        c.execute("DELETE FROM mwa_staging")

    local_db_conn.commit()


def insert_stats_into_local_db(data_list, local_db_connection):
    with closing(local_db_connection.cursor()) as c:
        c.executemany('INSERT INTO mwa_staging (stage_date_time, stage_seconds) VALUES (?, ?) ', data_list)

    local_db_connection.commit()


# Lines we are interested in look like:
# "2016-08-03 09:22:51,129, INFO, stageMultipleFiles finished staging. File hash: 1863432148 Staging time: 0.31 secs"
def parse_mwadmget_log(filename, local_db_conn):
    data_list = []

    staging_time_token = "Staging time:"

    with open(filename, "r") as mwa_dmget_log:
        line_no = 1

        for line in mwa_dmget_log:
            if staging_time_token in line:
                #
                # parse line
                #
                # 1. Get the date time from the first characters
                staging_date_time = datetime.strptime(line[0:19], "%Y-%m-%d %H:%M:%S")

                # 2. Get staging time in seconds
                # Find the token
                index = line.find(staging_time_token)

                if index == -1:
                    print(f"Line: {line_no} cannot parse- staging time not found!")
                    continue
                else:
                    # Get the time
                    start_pos = index + len(staging_time_token)
                    end_pos = len(line) - 6  # subtract " secs"

                    try:
                        staging_seconds = float(line[start_pos:end_pos])

                        if staging_seconds < 1:
                            staging_seconds = int(1)
                        else:
                            staging_seconds = int(staging_seconds)
                    except ValueError as e:
                        print(f"Line: {line_no} cannot parse! {e}")
                        continue

                    # Add to data structure
                    data_list.append( (staging_date_time, staging_seconds) )

            line_no = line_no + 1

    insert_stats_into_local_db(data_list, local_db_conn)

    return len(data_list)


if __name__ == "__main__":
    #
    # Usage: python staging_scraping.py --logpath=<path to logfiles>
    #
    local_db_conn = sqlite3.connect('mwa_staging_sqlite.db')

    print("Creating table...")
    #create_table(local_db_conn)

    print("Parsing log...")
    #rows = parse_mwadmget_log("mwadmget.log", local_db_conn)
    #print(f"Parsed {rows} of staging times from log.")

    date_from = datetime(2018, 1, 25)
    date_to = datetime(2019, 1, 25)
    do_plot_histogram(local_db_conn, date_from, date_to, "Staging time historgram", min_seconds=3, max_seconds=3600)
    exit(0)
