import logging
from collections import defaultdict
from typing import LiteralString, cast

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

from core import (
    MEDIAN_QUANTILE,
    FileSizeBin,
    FileTypeStats,
    build_file_size_bin_edges,
    build_file_size_bins,
    build_filetype_slice_where_clauses,
    format_file_type_label,
    get_filetype_order,
)

logger = logging.getLogger("archive_stats")

DB_POOL_MIN_SIZE = 1
DB_POOL_MAX_SIZE = 2


def get_connection_pool(host, user, database, password, port) -> ConnectionPool:
    """Create a thread-safe connection pool for the MWA database.

    Args:
        host: Database host name.
        user: Database user name.
        database: Database name.
        password: Database password.
        port: Database port.

    Returns:
        An open psycopg_pool.ConnectionPool instance with its minimum number of connections ready.

    Raises:
        psycopg_pool.PoolTimeout: If the database cannot be reached.
    """
    pool = ConnectionPool(
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        kwargs={
            "host": host,
            "user": user,
            "dbname": database,
            "password": password,
            "port": port,
        },
        open=False,
    )
    pool.open(wait=True)
    return pool


def fetch_daily_deleted_data_from_db(mwa_db: ConnectionPool):
    """Fetch all deleted-data daily/project aggregates from the database (no date filter).

    Returns a list of dicts with keys: reporting_year, reporting_month, reporting_date, projectid, deleted_bytes
    """
    logger.info("Fetching deleted data by day and projectid from database...")
    results = None

    try:
        with mwa_db.connection() as conn:
            cursor = conn.execute(
                """
                SELECT
                    date_part('year', deleted_timestamp) as reporting_year
                    ,date_part('month', deleted_timestamp) as reporting_month
                    ,date_part('day', deleted_timestamp) as reporting_day
                    ,projectid
                    ,COALESCE(SUM(size), 0) as deleted_bytes
                FROM data_files
                WHERE deleted_timestamp IS NOT NULL                 
                GROUP BY 1, 2, 3, 4
                ORDER BY 1, 2, 3, 4
                """
            )
            results = cursor.fetchall()
    except (psycopg.DatabaseError, psycopg.OperationalError) as error:
        logger.error(error)

    if results is None:
        return []

    return {(int(row[0]), int(row[1]), int(row[2]), row[3]): int(row[4]) for row in results}


def build_monthly_deleted_lookup(daily_deleted_data) -> dict[tuple[int, int], int]:
    """Sum deleted bytes per (year, month).

    Args:
        daily: Deleted bytes keyed by (year, month, day, project_id).

    Returns:
        Deleted bytes keyed by (year, month).
    """
    monthly: defaultdict[tuple[int, int], int] = defaultdict(int)
    for (year, month, _day, _project_id), deleted_bytes in daily_deleted_data.items():
        monthly[(year, month)] += deleted_bytes
    return dict(monthly)


def fetch_file_type_stats_from_db(mwa_db: ConnectionPool) -> list[FileTypeStats]:
    """Fetch size statistics per data type from the MWA database.

    Excludes deleted files.

    Args:
        mwa_db: A psycopg connection pool for the MWA database.

    Returns:
        One FileTypeStats per data type. Raw correlator files are split by era.
        It is sorted by display order.
        An empty list if the query failed.
    """
    logger.info("Fetching file type statistics from database...")
    stats = []

    try:
        with mwa_db.connection() as conn, conn.cursor() as cursor:
            for era_label, where_clause, display_order_modifier in build_filetype_slice_where_clauses(
                "f.observation_num", "f.filetype"
            ):
                logger.info(f"...Fetching statistics for era: {era_label}")
                # where_clause is built from internal constants only (see core.build_filetype_slice_where_clauses).
                where_sql = sql.SQL(cast(LiteralString, where_clause))
                cursor.execute(
                    t"""
                    SELECT
                        f.filetype
                        ,t.description
                        ,COUNT(*)
                        ,MIN(f.size)
                        ,AVG(f.size)
                        ,percentile_disc({MEDIAN_QUANTILE:l}) WITHIN GROUP (ORDER BY f.size)
                        ,MAX(f.size)
                        ,SUM(f.size)
                    FROM data_files f
                    INNER JOIN data_file_types t ON t.type = f.filetype
                    WHERE {where_sql:q} AND remote_archived=True AND deleted_timestamp IS NULL
                    GROUP BY f.filetype, t.description
                    ORDER BY f.filetype
                    """
                )
                for row in cursor.fetchall():
                    try:
                        filetype_order = get_filetype_order().index(int(row[0]))
                    except ValueError:
                        filetype_order = 999  # unknown file type, put it at the end
                        logger.warning(
                            f"Unknown file type {row[0]} found in database, putting it at the end of the table."
                        )

                    stats.append(
                        FileTypeStats(
                            label=format_file_type_label(int(row[0]), row[1], era_label),
                            filetype=int(row[0]),
                            file_count=int(row[2]),
                            min_bytes=int(row[3]),
                            mean_bytes=float(row[4]),
                            median_bytes=int(row[5]),
                            max_bytes=int(row[6]),
                            total_bytes=int(row[7]),
                            display_order=filetype_order + display_order_modifier,
                        )
                    )
    except (psycopg.DatabaseError, psycopg.OperationalError) as error:
        logger.error(error)
        return []

    # sort the stats by display_order
    stats.sort(key=lambda s: s.display_order)

    return stats


def fetch_file_size_histogram_from_db(mwa_db: ConnectionPool) -> list[FileSizeBin]:
    """Fetch the number of files per file size range from the MWA database.

    The files are counted in the database, so the rows of the data_files table are not transferred. Excludes deleted
    files and files that are not archived remotely.

    Args:
        mwa_db: A psycopg connection pool for the MWA database.

    Returns:
        One FileSizeBin per size range, smallest files first, including the ranges that have no files. An empty list
        if the query failed.
    """
    logger.info("Fetching file size histogram from database...")
    edges = build_file_size_bin_edges()

    try:
        with mwa_db.connection() as conn:
            cursor = conn.execute(
                t"""
                SELECT
                    width_bucket(size, {edges}::bigint[]) AS bucket
                    ,COUNT(*)
                FROM data_files
                WHERE remote_archived=True AND deleted_timestamp IS NULL AND size IS NOT NULL
                GROUP BY 1
                ORDER BY 1
                """
            )
            bucket_counts = {int(row[0]): int(row[1]) for row in cursor.fetchall()}
    except (psycopg.DatabaseError, psycopg.OperationalError) as error:
        logger.error(error)
        return []

    return build_file_size_bins(edges, bucket_counts)
