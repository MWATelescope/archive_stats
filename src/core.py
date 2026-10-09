from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from astropy.time import Time

AWST = ZoneInfo("Australia/Perth")
LEGACY_FP32_START = datetime(2013, 7, 1, tzinfo=AWST)
LEGACY_INT32_START = datetime(2014, 10, 15, tzinfo=AWST)
LEGACY_INT32_COMPRESSED_START = datetime(2017, 11, 1, tzinfo=AWST)
CORRELATOR_FILETYPE = 8

BYTES_PER_KB = 1000.0
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

# File size histogram bins: the lower edges are mantissa * 10**exponent bytes, e.g. 1 B, 2 B, 5 B, 10 B, ... 5 TB.
# Files of 0 bytes fall below the first edge, and files at or above the last edge fall into the last bin.
FILE_SIZE_BIN_MANTISSAS = (1, 2, 5)
FILE_SIZE_BIN_MIN_EXPONENT = 0
FILE_SIZE_BIN_MAX_EXPONENT = 12
FILE_SIZE_PLOT_Y_MARGIN = 0.2  # Fraction of the count axis range kept free for the count labels
FILE_SIZE_UNITS = (
    ("B", 1.0),
    ("kB", BYTES_PER_KB),
    ("MB", BYTES_PER_MB),
    ("GB", BYTES_PER_GB),
    ("TB", BYTES_PER_TB),
    ("PB", BYTES_PER_PB),
)


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
    filetype: int
    file_count: int
    min_bytes: int
    mean_bytes: float
    median_bytes: int
    max_bytes: int
    total_bytes: int
    display_order: int


@dataclass(frozen=True)
class FileSizeBin:
    """Number of files in one file size range. All sizes are in bytes."""

    lower_bytes: int
    upper_bytes: int | None
    file_count: int


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


def build_filetype_slice_where_clauses(obsid_column: str, filetype_column: str) -> list[tuple[str, str, int]]:
    """Build the WHERE expressions that split the files into the rows of the tables.

    Args:
        obsid_column: Name of the observation id (GPS seconds) column, as it is used in the query.
        filetype_column: Name of the file type column, as it is used in the query.

    Returns:
        A list of (era label, WHERE expression, display order modifier). The first entry is every file type except raw
        correlator, with an empty era label. The others are one per raw correlator era.
    """
    # The first slice is all non-correlator files, which are not split by era.
    # The display order modifier is 100 so that they appear after the correlator eras (which are 1, 2, 3, ...).
    slices = [("", f"{filetype_column} <> {CORRELATOR_FILETYPE}", 100)]

    for i, era in enumerate(build_correlator_eras(), start=1):
        conditions = [f"{filetype_column} = {CORRELATOR_FILETYPE}"]
        if era.gps_start is not None:
            conditions.append(f"{obsid_column} >= {era.gps_start}")
        if era.gps_end is not None:
            conditions.append(f"{obsid_column} < {era.gps_end}")
        slices.append((era.label, " AND ".join(conditions), i))
    return slices


def get_filetype_order() -> list[int]:
    """Get the file types in the order they should be displayed in tables and plots.

    Returns:
        The file types, oldest first.
    """
    return [CORRELATOR_FILETYPE, 18, 11, 16, 15, 17, 20, 19, 14, 10]


def format_file_type_label(filetype: int, description: str | None, era_label: str) -> str:
    """Build the Data Type label for a table row."""
    label = f"{filetype} - {description if description else 'unknown'}"
    return f"{label} [{era_label}]" if era_label else label


def build_file_size_bin_edges() -> list[int]:
    """Build the lower edges of the file size histogram bins.

    Returns:
        The edges in bytes, in ascending order (1-2-5 steps per decade).
    """
    return [
        mantissa * 10**exponent
        for exponent in range(FILE_SIZE_BIN_MIN_EXPONENT, FILE_SIZE_BIN_MAX_EXPONENT + 1)
        for mantissa in FILE_SIZE_BIN_MANTISSAS
    ]


def build_file_size_bins(edges: list[int], bucket_counts: dict[int, int]) -> list[FileSizeBin]:
    """Build the file size histogram bins, including the bins that have no files.

    Bucket numbers follow the PostgreSQL width_bucket() function: bucket 0 holds the files below the first edge,
    bucket N (N >= 1) holds the files from edges[N - 1] up to, but not including, edges[N]. The last bucket has no
    upper limit.

    Args:
        edges: The lower edges in bytes, in ascending order.
        bucket_counts: The number of files for each bucket number. Missing buckets have no files.

    Returns:
        One FileSizeBin per bucket, smallest files first.
    """
    bins = []
    for bucket in range(len(edges) + 1):
        lower_bytes = edges[bucket - 1] if bucket > 0 else 0
        upper_bytes = edges[bucket] if bucket < len(edges) else None
        bins.append(FileSizeBin(lower_bytes, upper_bytes, bucket_counts.get(bucket, 0)))
    return bins


def format_file_size(size_bytes: float) -> str:
    """Format a size in bytes using the largest decimal unit that fits, e.g. 2000000 -> '2 MB'."""
    unit_name, unit_bytes = FILE_SIZE_UNITS[0]
    for name, unit_size in FILE_SIZE_UNITS:
        if size_bytes >= unit_size:
            unit_name, unit_bytes = name, unit_size
    return f"{size_bytes / unit_bytes:g} {unit_name}"


def format_file_size_bin_label(size_bin: FileSizeBin) -> str:
    """Build the axis label of a file size histogram bin."""
    if size_bin.upper_bytes is None:
        return f">= {format_file_size(size_bin.lower_bytes)}"
    if size_bin.lower_bytes == 0:
        return "0 B (empty)"
    return f"{format_file_size(size_bin.lower_bytes)} - {format_file_size(size_bin.upper_bytes)}"


def filter_monthly_by_range(monthly_data, date_from, date_to):
    """Filter monthly rows to those whose (year, month) falls within the date range."""
    from_ym = (date_from.year, date_from.month)
    to_ym = (date_to.year, date_to.month)
    return [row for row in monthly_data if from_ym <= (row["reporting_year"], row["reporting_month"]) <= to_ym]


def build_deleted_lookup(deleted_data):
    """Build a {(year, month): deleted_bytes} dict for O(1) lookups."""
    return {(row["reporting_year"], row["reporting_month"]): row["deleted_bytes"] for row in deleted_data}
