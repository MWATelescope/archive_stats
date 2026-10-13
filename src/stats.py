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
    to_date,
)

logger = logging.getLogger("archive_stats")


def build_yearly_summary(monthly_data, monthly_deleted_lookup):
    """Aggregate ingest and deleted bytes per calendar year.

    Args:
        monthly_data: Rows from fetch_monthly_data_from_tap().
        monthly_deleted_lookup: {(year, month): deleted_bytes} from build_monthly_deleted_lookup().

    Returns:
        A list of dicts with keys: year, ingested_bytes, archived_bytes, deleted_bytes, net_bytes, total_bytes.
        total_bytes is the cumulative net volume at the end of that year.
    """
    ingest_by_year: dict[int, int] = defaultdict(int)
    deleted_by_year: dict[int, int] = defaultdict(int)

    for row in monthly_data:
        ingest_by_year[row["reporting_year"]] += row["total_ingested_bytes"]

    for (year, _month), deleted_bytes in monthly_deleted_lookup.items():
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


def print_yearly_summary(title: str, monthly_data, monthly_deleted_lookup):
    """Print a per-year table of ingest, deleted, net and total volume to the console.

    The current year is marked with '*' because its total is as at today, not 31-Dec.

    Args:
        title: Title for the table
        monthly_data: Rows from fetch_monthly_data_from_tap().
        monthly_deleted_lookup: {(year, month): deleted_bytes} from build_monthly_deleted_lookup().
    """
    current_year = datetime.now(tz=AWST).year
    header = YEARLY_TABLE_ROW_FORMAT.format(
        year="Year", ingest="Ingest (TB)", deleted="Deleted (TB)", net="Net (TB)", total="Total at 31-Dec (TB)"
    )
    separator = "-" * len(header)

    print(f"\n{title}")
    print(header)
    print(separator)
    for row in build_yearly_summary(monthly_data, monthly_deleted_lookup):
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

    Ingested bytes are by observation date and deleted bytes are by deletion date (both AWST), so the two columns
    are not netted per row. A deletion is written once, on the first row for its (date, projectid). Deletions on a
    (date, projectid) with no observation are written as extra rows at the end, so no deleted bytes are lost.

    Args:
        daily_data: Raw pyvo result set from fetch_daily_stats_from_tap().
        daily_deleted_data: {(deletion date, projectid): deleted_bytes} from fetch_daily_deleted_data_from_db().
        filename: The CSV file to write.
    """
    i = 0
    total_ingested_bytes = 0
    total_deleted_bytes = 0
    total_secs = 0

    # Each deletion is written once, so remove it from here when used.
    unwritten_deleted_data = dict(daily_deleted_data)

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
            "ingested(bytes)",
            "deleted(bytes)",
            "time(hours)",
            "ingested(TB)",
            "deleted(TB)",
        )

        stats_csv_writer.writerow(header)

        for row in daily_data:
            i += 1
            row_date = to_date(row["reporting_date"])

            secs = int(row["total_time_secs"]) if row["total_time_secs"] is not None else 0
            ingested_bytes = int(row["total_ingested_bytes"]) if row["total_ingested_bytes"] is not None else 0
            deleted_bytes = unwritten_deleted_data.pop((row_date, row["projectid"]), 0)

            total_secs += secs
            total_ingested_bytes += ingested_bytes
            total_deleted_bytes += deleted_bytes

            stats_csv_writer.writerow(
                (
                    row_date,
                    row["projectid"],
                    row["mwa_array_configuration"],
                    secs,
                    ingested_bytes,
                    deleted_bytes,
                    secs / 3600,
                    bytes_to_terabytes(ingested_bytes),
                    bytes_to_terabytes(deleted_bytes),
                )
            )

        # Deletions on a date/project with no observations still need to be reported.
        for (deleted_date, project_id), deleted_bytes in sorted(
            unwritten_deleted_data.items(), key=lambda item: (item[0][0], item[0][1] or "")
        ):
            i += 1
            total_deleted_bytes += deleted_bytes
            stats_csv_writer.writerow(
                (deleted_date, project_id, "", 0, 0, deleted_bytes, 0.0, 0.0, bytes_to_terabytes(deleted_bytes))
            )

    logger.info(f"{i} rows written to {filename}.")
    logger.info(f"Total ingested data: {bytes_to_petabytes(total_ingested_bytes)} PB")
    logger.info(f"Total deleted data: {bytes_to_petabytes(total_deleted_bytes)} PB")
    logger.info(f"Total net data: {bytes_to_petabytes(total_ingested_bytes - total_deleted_bytes)} PB")
    logger.info(f"Total time: {total_secs / 3600} hours")


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


def write_monthly_stats(monthly_data, monthly_deleted_lookup, filename):
    """Write monthly stats to a CSV file.

    Args:
        monthly_data: Rows from fetch_monthly_data_from_tap(), including deletion-only months
            (see add_deletion_only_months()).
        monthly_deleted_lookup: {(year, month): deleted_bytes} from build_monthly_deleted_lookup().
        filename: The CSV file to write.
    """
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

            deleted_size = monthly_deleted_lookup.get((year, month), 0)

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
