import argparse
import logging
import os
import sys
from configparser import ConfigParser
from datetime import datetime

from dateutil.relativedelta import relativedelta

from core import AWST, build_deleted_lookup, bytes_to_terabytes
from data_db import (
    fetch_deleted_data_by_month_from_db,
    fetch_file_size_histogram_from_db,
    fetch_file_type_stats_from_db,
    get_connection_pool,
)
from data_tap import (
    fetch_archive_bytes_by_mode_from_tap,
    fetch_daily_stats_from_tap,
    fetch_location_summary_bytes_from_tap,
    fetch_monthly_data_from_tap,
    fetch_project_data_from_tap,
    get_tap_service,
)
from pawsey import get_acacia_usage, get_banksia_usage
from plots import (
    plot_archive_volume_per_data_type,
    plot_archive_volume_per_month,
    plot_archive_volume_per_project,
    plot_download_count_per_month,
    plot_downloaded_data_per_month,
    plot_file_size_distribution,
    plot_telescope_time_per_project,
)
from stats import (
    print_file_type_table,
    print_yearly_summary,
    write_daily_stats,
    write_monthly_stats,
    write_stats_by_project,
)

logger = logging.getLogger("archive_stats")
logger.setLevel(logging.DEBUG)
console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
logger.addHandler(console_log)


def run(config_filename, no_pawsey: bool = False):
    """Main function"""
    # Usage: python stats.py --cfg=path/to/config/file

    config = ConfigParser()
    config.read(config_filename)

    minio_path = config.get("S3", "minio_path")
    if not os.path.exists(minio_path):
        print(f"Path to minio not valid: {minio_path}")
        sys.exit(1)

    mwa_db = get_connection_pool(
        host=config.get("MWA Database", "dbhost"),
        user=config.get("MWA Database", "dbuser"),
        database=config.get("MWA Database", "dbname"),
        password=config.get("MWA Database", "dbpass"),
        port=config.getint("MWA Database", "dbport"),
    )

    acacia_mwa_quota_bytes = config.getint("asvo_stats", "acacia_mwa_quota_bytes")
    banksia_quota_bytes = config.getint("asvo_stats", "banksia_quota_bytes")

    tap_url = config.get("MWA TAP", "url")
    mwa_tap_service = get_tap_service(tap_url)

    # Example of the expected format is: 01-Jul-2022
    # NOTE: keeping these in case I have a use for them later!
    special_date_from = datetime.strptime(config.get("asvo_stats", "special_date_from"), "%d-%b-%Y").astimezone(AWST)
    special_date_to = datetime.strptime(config.get("asvo_stats", "special_date_to"), "%d-%b-%Y").astimezone(AWST)

    today = datetime.now(tz=AWST)
    start_date = datetime(2006, 1, 1, tzinfo=AWST)

    six_months_ago = today - relativedelta(months=6)

    # --- Location summary (TAP) ---
    logger.info("Getting summary stats from database...")
    (
        db_acacia_mwa_bytes,
        db_banksia_bytes,
    ) = fetch_location_summary_bytes_from_tap(mwa_tap_service)

    logger.info(f"Acacia_mwa Quota Used: {bytes_to_terabytes(db_acacia_mwa_bytes):.3f} TB")
    logger.info(f"Banksia Quota Used: {bytes_to_terabytes(db_banksia_bytes):.3f} TB")

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

    logger.info("-------------------------------------------------------")

    # ---------------------------------------------------------------
    # Fetch data once
    # ---------------------------------------------------------------
    db_filetype_data = fetch_file_type_stats_from_db(mwa_db)
    tap_monthly_data = fetch_monthly_data_from_tap(mwa_tap_service)
    tap_daily_data = fetch_daily_stats_from_tap(mwa_tap_service)
    tap_project_data_all = fetch_project_data_from_tap(mwa_tap_service)
    tap_project_data_6mo = fetch_project_data_from_tap(mwa_tap_service, six_months_ago, today)
    tap_mode_data = fetch_archive_bytes_by_mode_from_tap(mwa_tap_service)
    db_deleted_data = fetch_deleted_data_by_month_from_db(mwa_db)
    db_file_size_histogram = fetch_file_size_histogram_from_db(mwa_db)

    # Close MWA database
    mwa_db.close()

    # Filter data
    db_deleted_lookup = build_deleted_lookup(db_deleted_data)

    logger.info(
        f"Fetched {len(tap_monthly_data)} monthly rows, "
        f"{len(tap_project_data_all)} projects (all time), "
        f"{len(tap_project_data_6mo)} projects (6 mo), "
        f"{len(db_deleted_data)} deleted-data months, "
        f"{len(tap_mode_data)} modes."
    )

    # ---------------------------------------------------------------
    # CSV dumps (use pre-fetched data)
    # ---------------------------------------------------------------
    print_file_type_table("File type summary from the MWA database (data_files)", db_filetype_data)
    print_yearly_summary("Yearly Summary from ()", tap_monthly_data, db_deleted_lookup)
    write_daily_stats(tap_daily_data, "stats.csv")
    write_monthly_stats(tap_monthly_data, "stats_by_month.csv")
    write_stats_by_project(tap_project_data_all, "stats_by_project.csv")

    # ---------------------------------------------------------------
    # Plots (use pre-fetched data, filter in Python)
    # ---------------------------------------------------------------

    # Downloaded data
    plot_downloaded_data_per_month(
        tap_monthly_data,
        start_date,
        today,
        "Downloaded Data per month (all time)",
        False,
        "mwa_downloaded_data_per_month_all_time.png",
    )

    plot_downloaded_data_per_month(
        tap_monthly_data,
        start_date,
        today,
        "Cumulative downloaded data per month (all time)",
        True,
        "mwa_downloaded_data_per_month_all_time_cuml.png",
    )

    # Download counts
    plot_download_count_per_month(
        tap_monthly_data,
        start_date,
        today,
        "Number of Downloads per month (all time)",
        False,
        "mwa_download_count_per_month_all_time.png",
    )

    plot_download_count_per_month(
        tap_monthly_data,
        start_date,
        today,
        "Cumulative Number of Downloads per month (all time)",
        True,
        "mwa_download_count_per_month_all_time_cuml.png",
    )

    # Archive volume — all time
    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        start_date,
        today,
        "MWA Archive Volume (all time)",
        True,
        "mwa_archive_volume_all_time.png",
        False,
    )
    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        start_date,
        today,
        "MWA Archive Ingest (all time)",
        True,
        "mwa_archive_ingest_all_time_cuml.png",
        True,
    )

    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        special_date_from,
        special_date_to,
        f"MWA Archive Ingest ({special_date_from} - {special_date_to})",
        False,
        "mwa_archive_ingest_special.png",
        True,
    )

    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        special_date_from,
        special_date_to,
        f"MWA Archive Ingest ({special_date_from} - {special_date_to})",
        True,
        "mwa_archive_ingest_special_cuml.png",
        True,
    )

    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        special_date_from,
        special_date_to,
        f"MWA Archive Volume ({special_date_from} - {special_date_to})",
        False,
        "mwa_archive_volume_special.png",
        False,
    )

    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        special_date_from,
        special_date_to,
        f"MWA Archive Volume ({special_date_from} - {special_date_to})",
        True,
        "mwa_archive_volume_special_cuml.png",
        False,
    )

    # Per-project — all time
    plot_archive_volume_per_project(
        tap_project_data_all,
        "MWA Archive Volume by Project (all time)",
        "mwa_archive_volume_by_project_all_time.png",
    )
    plot_telescope_time_per_project(
        tap_project_data_all,
        "MWA Telescope Time (all time)",
        "mwa_telescope_time_all_time.png",
    )

    # Archive volume by data type
    plot_archive_volume_per_data_type(
        tap_mode_data,
        "MWA Archive Volume by Data Type",
        "mwa_archive_volume_by_data_type.png",
    )

    # File size distribution
    plot_file_size_distribution(
        db_file_size_histogram,
        "MWA Archive File Size Distribution",
        "mwa_archive_file_size_distribution.png",
    )

    # Archive volume — last 6 months
    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        six_months_ago,
        today,
        "MWA Archive Net Growth (last 6 months)",
        False,
        "mwa_archive_net_growth_last_6_months.png",
        False,
    )
    plot_archive_volume_per_month(
        tap_monthly_data,
        db_deleted_lookup,
        six_months_ago,
        today,
        "MWA Archive Ingest (last 6 months)",
        False,
        "mwa_archive_ingest_last_6_months.png",
        True,
    )

    # Per-project — last 6 months
    plot_archive_volume_per_project(
        tap_project_data_6mo,
        "MWA Archive Volume by Project (last 6 months)",
        "mwa_archive_volume_by_project_last_6_months.png",
    )
    plot_telescope_time_per_project(
        tap_project_data_6mo,
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

    run(args["cfg"], args["no_pawsey"])
