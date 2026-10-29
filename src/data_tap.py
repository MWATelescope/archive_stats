import logging

import pyvo as vo

logger = logging.getLogger("archive_stats")

ADQL_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_tap_service(tap_url: str) -> vo.dal.TAPService:
    return vo.dal.TAPService(tap_url)


def do_adql_query(vo_service: vo.dal.TAPService, adql_statement: str):
    """Given a VO service object, run the ADQL and return the results"""
    return vo_service.search(adql_statement)


def fetch_monthly_data_from_tap(tap_service: vo.dal.TAPService):
    """Fetch all monthly aggregates in a single TAP query (no date filter).

    All months are AWST: the *_mro columns are local time at the MRO (UTC+8).
    Observation-side aggregates are grouped by observation date (starttime_mro).
    Download bytes come from obs_download_history grouped by actual download
    date (datetime_mro), joined via FULL OUTER JOIN so months that appear in
    only one table are still included.

    Here we use the files_deleted_bytes simply to infer the ingested bytes
    (total_archived_bytes + files_deleted_bytes).

    To report on deletions by date deleted, we need to use the MWA database
    as it is not correct to associate a deletion with the observation date of the original file.

    Returns a list of dicts with keys:
        reporting_year, reporting_month, total_secs,
        total_ingested_bytes, downloaded_bytes,
        download_count
    """
    logger.info("Fetching monthly data from TAP...")
    results = do_adql_query(
        tap_service,
        """SELECT
                 COALESCE(o.reporting_year, d.reporting_year) as reporting_year
                ,COALESCE(o.reporting_month, d.reporting_month) as reporting_month
                ,COALESCE(o.total_secs, 0) as total_secs                
                ,COALESCE(o.archived_bytes, 0)+COALESCE(o.deleted_bytes, 0) as total_ingested_bytes
                ,COALESCE(d.downloaded_bytes, 0) as downloaded_bytes
                ,COALESCE(d.download_count, 0) as download_count
            FROM (
                SELECT
                     date_part('year', starttime_mro) as reporting_year
                    ,date_part('month', starttime_mro) as reporting_month
                    ,SUM(duration) as total_secs
                    ,SUM(total_archived_bytes) as archived_bytes
                    ,SUM(files_deleted_bytes) as deleted_bytes
                FROM mwa.observation                
                GROUP BY 1, 2
            ) o
            FULL OUTER JOIN (
                SELECT
                     date_part('year', datetime_mro) as reporting_year
                    ,date_part('month', datetime_mro) as reporting_month
                    ,SUM(downloaded_bytes) as downloaded_bytes
                    ,COUNT(*) as download_count
                FROM mwa.obs_download_history                
                GROUP BY 1, 2
            ) d ON o.reporting_year = d.reporting_year AND o.reporting_month = d.reporting_month
            ORDER BY 1, 2""",
    )
    return_results = [
        {
            "reporting_year": int(row["reporting_year"]),
            "reporting_month": int(row["reporting_month"]),
            "total_secs": int(row["total_secs"]),
            "total_ingested_bytes": int(row["total_ingested_bytes"]),
            "downloaded_bytes": int(row["downloaded_bytes"]),
            "download_count": int(row["download_count"]),
        }
        for row in results
    ]

    return return_results


def fetch_project_data_from_tap(tap_service: vo.dal.TAPService, date_from=None, date_to=None):
    """Fetch project-level aggregates (bytes and hours).

    If date_from/date_to are provided, restrict to that range.
    Returns a list of dicts with keys:
        projectid, projectshortname, total_archived_bytes, totaltime_hours
    """
    where_clause = ""
    if date_from is not None and date_to is not None:
        # starttime_mro is AWST wall time, so format the AWST datetimes as wall time with no UTC offset.
        where_clause = (
            f"WHERE starttime_mro BETWEEN '{date_from:{ADQL_TIMESTAMP_FORMAT}}' AND '{date_to:{ADQL_TIMESTAMP_FORMAT}}'"
        )

    label = f" ({date_from} to {date_to})" if date_from else " (all time)"
    logger.info(f"Fetching project data from TAP{label}...")

    results = do_adql_query(
        tap_service,
        f"""SELECT
                projectid
                ,projectshortname
                ,COALESCE(SUM(total_archived_bytes), 0) as total_archived_bytes
                ,COALESCE(SUM(duration), 0) / 3600 as totaltime_hours
            FROM mwa.observation
            {where_clause}
            GROUP BY projectid, projectshortname""",
    )
    return [
        {
            "projectid": row["projectid"],
            "projectshortname": row["projectshortname"],
            "total_archived_bytes": int(row["total_archived_bytes"]),
            "totaltime_hours": int(row["totaltime_hours"]),
        }
        for row in results
    ]


def fetch_daily_stats_from_tap(tap_service: vo.dal.TAPService):
    """Fetch daily stats by project/config for the CSV dump.

    Days are AWST (starttime_mro). Each row is one (date, projectid, config) group.

    This is the finest-grained query and can't be shared with plots,
    so it remains its own TAP call.
    """
    logger.info("Fetching daily stats from TAP...")
    return do_adql_query(
        tap_service,
        """SELECT
                date_trunc('day', starttime_mro) As reporting_date
                ,projectid
                ,mwa_array_configuration
                ,SUM(duration) as total_time_secs
                ,COALESCE(SUM(total_archived_bytes),0) + COALESCE(SUM(files_deleted_bytes),0) as total_ingested_bytes
            FROM mwa.observation
            GROUP BY 1,2,3
            ORDER BY 1,2""",
    )


def fetch_archive_bytes_by_mode_from_tap(tap_service: vo.dal.TAPService):
    """Fetch total archived data bytes grouped by observation mode.

    Returns a list of dicts with keys: mode, total_bytes
    """
    logger.info("Fetching archive bytes by mode from TAP...")
    results = do_adql_query(
        tap_service,
        """SELECT
                mode
                ,COALESCE(SUM(total_archived_bytes), 0) as total_bytes
            FROM mwa.observation
            GROUP BY mode""",
    )
    return [
        {
            "mode": row["mode"],
            "total_bytes": int(row["total_bytes"]),
        }
        for row in results
    ]


def fetch_location_summary_bytes_from_tap(tap_service: vo.dal.TAPService):
    """Returns the bytes stored for acacia and banksia from the database"""
    logger.info("Running query to get location stats...")
    results = do_adql_query(
        tap_service,
        """SELECT COALESCE(SUM(archived_bytes_at_pawsey_banksia), 0) as banksia,
                                 COALESCE(SUM(archived_bytes_at_pawsey_acacia_mwa), 0) as acacia_mwa
                          FROM mwa.observation""",
    )

    acacia_mwa = int(results[0]["acacia_mwa"])
    banksia = int(results[0]["banksia"])

    return acacia_mwa, banksia


# def fetch_file_type_stats_from_tap(tap_service: vo.dal.TAPService) -> list[FileTypeStats]:
#     """Fetch size statistics per data type from the TAP service.

#     Args:
#         tap_service: A pyvo TAPService.

#     Returns:
#         One FileTypeStats per data type. Raw correlator files are split by era.
#     """
#     logger.info("Fetching file type statistics from TAP...")
#     stats = []
#     for era_label, where_clause in build_filetype_slice_where_clauses("obs_id", "filetype"):
#         logger.info(f"...Fetching statistics for era: {era_label}")
#         results = do_adql_query(
#             tap_service,
#             f"""SELECT
#                     filetype
#                     ,filetypedescription
#                     ,SUM(archived_files) AS file_count
#                     ,MIN(archived_size) AS min_size
#                     ,AVG(archived_size) AS mean_size
#                     ,MAX(archived_size) AS max_size
#                     ,SUM(archived_size) AS total_size
#                 FROM mwa.obs_data_file
#                 WHERE {where_clause}
#                 GROUP BY filetype, filetypedescription
#                 ORDER BY filetype""",
#         )
#         for row in results:
#             filetype = int(row["filetype"])
#             label = format_file_type_label(filetype, row["filetypedescription"], era_label)
#             logger.info(f"Finding median size for {label}...")
#             median_bytes = -1
#             stats.append(
#                 FileTypeStats(
#                     label=label,
#                     file_count=int(row["file_count"]),
#                     min_bytes=int(row["min_size"]),
#                     mean_bytes=float(row["mean_size"]),
#                     median_bytes=median_bytes,
#                     max_bytes=int(row["max_size"]),
#                     total_bytes=int(row["total_size"]),
#                 )
#             )
#     return stats
