"""This module is used to provide overall storage stats for the MWA Archive"""

import csv
import logging
from collections import defaultdict
from datetime import datetime

from core import (
    AWST,
    FILE_TYPE_TABLE_ROW_FORMAT,
    FILE_TYPE_TABLE_SIZE_DECIMALS,
    FILE_TYPE_TABLE_TOTAL_DECIMALS,
    YEARLY_TABLE_ROW_FORMAT,
    FileTypeStats,
    bytes_to_megabytes,
    bytes_to_petabytes,
    bytes_to_terabytes,
    get_available_hours,
    get_duty_cycle,
)

logger = logging.getLogger("archive_stats")


def build_yearly_summary(monthly_data, daily_deleted_data):
    """Aggregate ingest and deleted bytes per calendar year.

    Args:
        monthly_data: Rows from fetch_monthly_data().
        daily_deleted_data: {(year, month, day, projectid): deleted_bytes} from fetch_daily_deleted_data()

    Returns:
        A list of dicts with keys: year, ingest_bytes, deleted_bytes, net_bytes, total_bytes.
        total_bytes is the cumulative net volume at the end of that year.
    """
    ingest_by_year: dict[int, int] = defaultdict(int)
    deleted_by_year: dict[int, int] = defaultdict(int)

    for row in monthly_data:
        ingest_by_year[row["reporting_year"]] += row["total_ingested_bytes"]

    for (year, _month, _day, _projectid), deleted_bytes in daily_deleted_data.items():
        deleted_by_year[year] += deleted_bytes

    summary = []
    total_bytes = 0
    for year in sorted(set(ingest_by_year) | set(deleted_by_year)):
        net_bytes = ingest_by_year[year] - deleted_by_year[year]
        total_bytes += net_bytes
        summary.append(
            {
                "year": year,
                "ingested_bytes": ingest_by_year[year],
                "archived_bytes": ingest_by_year[year] - deleted_by_year[year],
                "deleted_bytes": deleted_by_year[year],
                "net_bytes": net_bytes,
                "total_bytes": total_bytes,
            }
        )
    return summary


# ---------------------------------------------------------------------------
# CSV dump functions — take pre-fetched data, not service handles
# ---------------------------------------------------------------------------
def print_file_type_table(title: str, stats: list[FileTypeStats]) -> None:
    """Print a table of file type statistics to the console, with sizes in TB.

    Args:
        title: Heading printed above the table.
        stats: Rows from fetch_file_type_stats_tap() or fetch_file_type_stats_db().
    """
    size_format = f",.{FILE_TYPE_TABLE_SIZE_DECIMALS}f"
    total_format = f",.{FILE_TYPE_TABLE_TOTAL_DECIMALS}f"
    header = FILE_TYPE_TABLE_ROW_FORMAT.format(
        label="Data Type",
        count="Count",
        min_mb="Min (MB)",
        mean_mb="Mean (MB)",
        median_mb="Median (MB)",
        max_mb="Max (MB)",
        total_tb="Total (TB)",
    )
    separator = "-" * len(header.expandtabs())

    print(f"\n{title}")
    print(header)
    print(separator)
    for row in stats:
        print(
            FILE_TYPE_TABLE_ROW_FORMAT.format(
                label=row.label,
                count=f"{row.file_count:,}",
                min_mb=f"{bytes_to_megabytes(row.min_bytes):{size_format}}",
                mean_mb=f"{bytes_to_megabytes(row.mean_bytes):{size_format}}",
                median_mb=f"{bytes_to_megabytes(row.median_bytes):{size_format}}",
                max_mb=f"{bytes_to_megabytes(row.max_bytes):{size_format}}",
                total_tb=f"{bytes_to_terabytes(row.total_bytes):{total_format}}",
            )
        )
    print(separator)
    print(
        FILE_TYPE_TABLE_ROW_FORMAT.format(
            label="All",
            count=f"{sum(row.file_count for row in stats):,}",
            min_mb="",
            mean_mb="",
            median_mb="",
            max_mb="",
            total_tb=f"{bytes_to_terabytes(sum(row.total_bytes for row in stats)):{total_format}}",
        )
    )


def print_yearly_summary(title: str, monthly_data, daily_deleted_data):
    """Print a per-year table of ingest, deleted, net and total volume to the console.

    The current year is marked with '*' because its total is as at today, not 31-Dec.

    Args:
        title: Title for the table
        monthly_data: Rows from fetch_monthly_data().
        daily_deleted_data: {(year, month, day, projectid): deleted_bytes} from fetch_daily_deleted_data().
    """
    current_year = datetime.now(tz=AWST).year
    header = YEARLY_TABLE_ROW_FORMAT.format(
        year="Year", ingest="Ingest (TB)", deleted="Deleted (TB)", net="Net (TB)", total="Total at 31-Dec (TB)"
    )
    separator = "-" * len(header)

    print(f"\n{title}")
    print(header)
    print(separator)
    for row in build_yearly_summary(monthly_data, daily_deleted_data):
        year_label = f"{row['year']}{'*' if row['year'] == current_year else ''}"
        print(
            YEARLY_TABLE_ROW_FORMAT.format(
                year=year_label,
                ingest=f"{bytes_to_terabytes(row['ingested_bytes']):,.3f}",
                deleted=f"{bytes_to_terabytes(row['deleted_bytes']):,.3f}",
                net=f"{bytes_to_terabytes(row['net_bytes']):,.3f}",
                total=f"{bytes_to_terabytes(row['total_bytes']):,.3f}",
            )
        )
    print(separator)
    print(f"* {current_year} total is as at {datetime.now(tz=AWST):%d-%b-%Y}, not 31-Dec.")


def write_daily_stats(daily_data, daily_deleted_data, filename):
    """Write daily stats to a CSV file.

    daily_data is the raw pyvo result set from fetch_daily_stats().
    daily_deleted_data is {(year, month, day, projectid): deleted_bytes} from fetch_daily_deleted_data_from_db().
    """
    i = 0
    total_bytes = 0.0
    total_deleted_bytes = 0.0
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

            this_deleted_bytes = daily_deleted_data.get(
                (
                    (row["reporting_date"]).year,
                    (row["reporting_date"]).month,
                    (row["reporting_date"]).day,
                    row["projectid"],
                ),
                0,
            )
            total_deleted_bytes += this_deleted_bytes

            if row["total_ingested_bytes"] is not None:
                this_bytes = int(row["total_ingested_bytes"]) - this_deleted_bytes
                total_bytes += this_bytes
                terabytes = bytes_to_terabytes(this_bytes)
            else:
                terabytes = 0.0

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
    logger.info(f"Total deleted data: {bytes_to_petabytes(total_deleted_bytes)} PB")


def write_stats_by_project(project_data, filename):
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


def write_monthly_stats(monthly_data, daily_deleted_data, filename):
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

            # Summarise the daily deleted data into year/month
            deleted_size: int = sum(
                deleted_bytes
                for (y, m, _d, _projid), deleted_bytes in daily_deleted_data.items()
                if y == year and m == month
            )

            hours = row["total_secs"] / 3600
            terabytes = bytes_to_terabytes(row["total_ingested_bytes"] - deleted_size)
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
