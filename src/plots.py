import logging
import time

import matplotlib.pyplot as plt
import numpy as np

from core import (
    DPI,
    FILE_SIZE_PLOT_Y_MARGIN,
    FileSizeBin,
    bytes_to_petabytes,
    bytes_to_terabytes,
    filter_monthly_by_range,
    format_file_size_bin_label,
)

logger = logging.getLogger("archive_stats")


def clear_plots():
    """Clear all open plots"""
    plt.close("all")


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
        net_bytes = row["total_ingested_bytes"]
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


def plot_downloaded_data_per_month(
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


def plot_download_count_per_month(
    monthly_data,
    date_from,
    date_to,
    title,
    cumulative,
    filename,
):
    """Plot number of downloads per month from pre-fetched data."""
    clear_plots()

    rows = filter_monthly_by_range(monthly_data, date_from, date_to)

    x_axis = []
    y_axis = []
    cumulative_count = 0

    stride_months = 2 if (date_to - date_from).days > (6 * 31) else 1
    stride_accumulator = 0
    stride_count = 0

    for row in rows:
        count = row["download_count"]
        cumulative_count += count
        stride_accumulator += count
        stride_count += 1

        # Check striding - step every N rows from start, not by calendar month parity
        if stride_count % stride_months == 0:
            x_axis.append(f"{row['reporting_year']:d}-{row['reporting_month']:02d}")

            if cumulative:
                y_axis.append(cumulative_count)
            else:
                y_axis.append(stride_accumulator)

            stride_accumulator = 0

    # Handle any trailing partial stride window so the last month(s) aren't dropped
    if stride_accumulator > 0 and stride_count % stride_months != 0:
        last = rows[-1]
        x_axis.append(f"{last['reporting_year']:d}-{last['reporting_month']:02d}")
        if cumulative:
            y_axis.append(cumulative_count)
        else:
            y_axis.append(stride_accumulator)

    fig, _ = plt.subplots()
    plt.bar(x_axis, y_axis)
    plt.title(f"{title} = {cumulative_count:,} (as at {time.strftime('%d-%b-%Y')})")
    plt.xlabel("Time")
    plt.xticks(rotation=90)
    plt.ylabel("Number of Downloads")
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
    fontsize = 14 if len(labels) < 10 else None
    axis.pie(
        x_values,
        labels=labels,
        autopct=lambda pct: pie_volume_format(pct, x_values),
        startangle=0,
        textprops={"fontsize": fontsize} if fontsize else {},
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
    fontsize = 14 if len(labels) < 10 else None
    axis.pie(
        x_values,
        labels=labels,
        autopct=lambda pct: pie_hours_format(pct, x_values),
        startangle=0,
        textprops={"fontsize": fontsize} if fontsize else {},
    )
    axis.axis("equal")
    plt.title(f"{title} by Project (as at {time.strftime('%d-%b-%Y')})")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")


def plot_archive_volume_per_data_type(mode_data, title, filename):
    """Plot archive data volume by data type (mode) as a pie chart.

    Categorises modes from fetch_archive_bytes_by_mode() into:
        Raw Visibilities  = HW_LFILES, MWAX_CORRELATOR
        Raw Voltages      = VOLTAGE_START, VOLTAGE_BUFFER, MWAX_VCS, MWAX_BUFFER
        Beamformed        = MWAX_BEAMFORMER
        Unknown           = anything else
    """
    clear_plots()

    raw_vis_modes = {"HW_LFILES", "MWAX_CORRELATOR"}
    raw_volt_modes = {"VOLTAGE_START", "VOLTAGE_BUFFER", "MWAX_VCS", "MWAX_BUFFER"}
    beamformed_modes = {"MWAX_BEAMFORMER"}

    raw_vis = 0
    raw_volt = 0
    beamformed = 0
    unknown = 0

    for row in mode_data:
        mode = row["mode"]
        total = row["total_bytes"]

        if mode in raw_vis_modes:
            raw_vis += total
        elif mode in raw_volt_modes:
            raw_volt += total
        elif mode in beamformed_modes:
            beamformed += total
        else:
            unknown += total

    labels = ["Raw Visibilities", "Raw Voltages", "Beamformed", "Unknown"]
    x_values = [
        bytes_to_terabytes(raw_vis),
        bytes_to_terabytes(raw_volt),
        bytes_to_terabytes(beamformed),
        bytes_to_terabytes(unknown),
    ]

    # Drop zero-valued slices so the chart isn't cluttered
    non_zero = [(label, val) for label, val in zip(labels, x_values) if val > 0]
    if not non_zero:
        logger.warning("No data-type volume to plot.")
        return
    labels, x_values = zip(*non_zero)

    fig, axis = plt.subplots()
    fontsize = 14 if len(labels) < 10 else None
    axis.pie(
        x_values,
        labels=labels,
        autopct=lambda pct: pie_volume_format(pct, x_values),
        startangle=0,
        textprops={"fontsize": fontsize} if fontsize else {},
    )
    axis.axis("equal")

    plt.title(f"{title} (as at {time.strftime('%d-%b-%Y')})")
    fig.set_size_inches(18.5, 10.5)
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")


def plot_file_size_distribution(size_bins: list[FileSizeBin], title: str, filename: str) -> None:
    """Plot the number of archived files per file size range as a histogram.

    Size ranges without files at the small and large ends are left out. The count axis is logarithmic, because the
    counts of the size ranges differ by orders of magnitude.

    Args:
        size_bins: The size ranges with their file counts, smallest files first.
        title: The plot title.
        filename: The image file to write.
    """
    clear_plots()

    populated = [index for index, size_bin in enumerate(size_bins) if size_bin.file_count > 0]
    if not populated:
        logger.warning("No file sizes to plot.")
        return
    size_bins = size_bins[populated[0] : populated[-1] + 1]

    counts = [size_bin.file_count for size_bin in size_bins]
    positions = np.arange(len(size_bins))

    fig, axis = plt.subplots()
    bars = axis.bar(positions, counts)
    # Empty size ranges inside the plotted range get no label, as zero cannot be drawn on a logarithmic axis.
    axis.bar_label(bars, labels=[f"{count:,}" if count > 0 else "" for count in counts], rotation=90, padding=3)
    # axis.set_yscale("log")
    axis.margins(y=FILE_SIZE_PLOT_Y_MARGIN)
    axis.set_xticks(
        positions, labels=[format_file_size_bin_label(size_bin) for size_bin in size_bins], rotation=45, ha="right"
    )
    axis.set_xlabel("File size")
    axis.set_ylabel("Number of files (log scale)")

    plt.title(f"{title} (as at {time.strftime('%d-%b-%Y')})")
    fig.set_size_inches(18.5, 10.5)
    plt.tight_layout()
    plt.savefig(filename, dpi=DPI)
    logger.info(f"Saved {filename} to disk.")
