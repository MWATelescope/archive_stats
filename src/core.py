from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from astropy.time import Time

AWST = ZoneInfo("Australia/Perth")
LEGACY_FP32_START = datetime(2013, 7, 1, tzinfo=AWST)
LEGACY_INT32_START = datetime(2014, 10, 15, tzinfo=AWST)
LEGACY_INT32_COMPRESSED_START = datetime(2017, 11, 1, tzinfo=AWST)
CORRELATOR_FILETYPE = 8

BYTES_PER_MB = 1000.0**2
BYTES_PER_GB = BYTES_PER_MB * 1000.0
BYTES_PER_TB = BYTES_PER_GB * 1000.0
BYTES_PER_PB = BYTES_PER_TB * 1000.0
YEARLY_TABLE_ROW_FORMAT = "{year:<6}\t{ingest:>14}\t{deleted:>14}\t{net:>14}\t{total:>18}"

DPI = 100

FILE_TYPE_TABLE_ROW_FORMAT = (
    "{label:<70}\t{count:>14}\t{min_mb:>14}\t{mean_mb:>14}\t{median_mb:>14}\t{max_mb:>14}\t{total_tb:>16}"
)
FILE_TYPE_TABLE_SIZE_DECIMALS = 3
FILE_TYPE_TABLE_TOTAL_DECIMALS = 3
MEDIAN_QUANTILE = 0.5


def datetime_to_gps_seconds(datetime_with_timezone: datetime) -> int:
    """Convert a timezone-aware datetime to GPS seconds.

    Args:
        when: A timezone-aware datetime.

    Returns:
        The GPS time in whole seconds.
    """
    return Time(datetime_with_timezone).gps


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


def bytes_to_megabytes(bytes_value):
    """Convert bytes to MB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / BYTES_PER_MB


def bytes_to_gigabytes(bytes_value):
    """Convert bytes to GB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / BYTES_PER_GB


def bytes_to_terabytes(bytes_value):
    """Convert bytes to TB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / BYTES_PER_TB


def bytes_to_petabytes(bytes_value):
    """Convert bytes to PB"""
    if bytes_value is None:
        return 0.0
    return float(bytes_value) / BYTES_PER_PB


@dataclass(frozen=True)
class CorrelatorEra:
    """A range of raw correlator observations, as GPS seconds (start inclusive, end exclusive)."""

    label: str
    gps_start: int | None
    gps_end: int | None


@dataclass(frozen=True)
class FileTypeStats:
    """Size statistics for one data type. All sizes are in bytes."""

    label: str
    file_count: int
    min_bytes: int
    mean_bytes: float
    median_bytes: int
    max_bytes: int
    total_bytes: int


def build_correlator_eras() -> list[CorrelatorEra]:
    """Build the date ranges that raw correlator files (filetype 8) are split into.

    The first era catches files before the first split date, so no file is silently dropped.

    Returns:
        The eras, oldest first.
    """
    fp32_start = datetime_to_gps_seconds(LEGACY_FP32_START)
    int32_start = datetime_to_gps_seconds(LEGACY_INT32_START)
    compressed_start = datetime_to_gps_seconds(LEGACY_INT32_COMPRESSED_START)
    return [
        CorrelatorEra(f"before {LEGACY_FP32_START:%d-%b-%Y}", None, fp32_start),
        CorrelatorEra("Legacy correlator fp32", fp32_start, int32_start),
        CorrelatorEra("Legacy correlator int32", int32_start, compressed_start),
        CorrelatorEra("Legacy correlator int32 compressed", compressed_start, None),
    ]


def build_filetype_slice_where_clauses(obsid_column: str, filetype_column: str) -> list[tuple[str, str]]:
    """Build the WHERE expressions that split the files into the rows of the tables.

    Args:
        obsid_column: Name of the observation id (GPS seconds) column, as it is used in the query.
        filetype_column: Name of the file type column, as it is used in the query.

    Returns:
        A list of (era label, WHERE expression). The first entry is every file type except raw
        correlator, with an empty era label. The others are one per raw correlator era.
    """
    slices = [("", f"{filetype_column} <> {CORRELATOR_FILETYPE}")]
    for era in build_correlator_eras():
        conditions = [f"{filetype_column} = {CORRELATOR_FILETYPE}"]
        if era.gps_start is not None:
            conditions.append(f"{obsid_column} >= {era.gps_start}")
        if era.gps_end is not None:
            conditions.append(f"{obsid_column} < {era.gps_end}")
        slices.append((era.label, " AND ".join(conditions)))
    return slices


def format_file_type_label(filetype: int, description: str | None, era_label: str) -> str:
    """Build the Data Type label for a table row."""
    label = f"{filetype} - {description if description else 'unknown'}"
    return f"{label} [{era_label}]" if era_label else label


def filter_monthly_by_range(monthly_data, date_from, date_to):
    """Filter monthly rows to those whose (year, month) falls within the date range."""
    from_ym = (date_from.year, date_from.month)
    to_ym = (date_to.year, date_to.month)
    return [row for row in monthly_data if from_ym <= (row["reporting_year"], row["reporting_month"]) <= to_ym]


def build_deleted_lookup(deleted_data):
    """Build a {(year, month): deleted_bytes} dict for O(1) lookups."""
    return {(row["reporting_year"], row["reporting_month"]): row["deleted_bytes"] for row in deleted_data}
