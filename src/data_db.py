import logging
from typing import LiteralString, cast

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

from core import MEDIAN_QUANTILE, FileTypeStats, build_filetype_slice_where_clauses, format_file_type_label

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


def fetch_deleted_data_by_month_from_db(mwa_db: ConnectionPool):
    """Fetch all deleted-data monthly aggregates from the database (no date filter).

    Returns a list of dicts with keys: reporting_year, reporting_month, deleted_bytes
    """
    logger.info("Fetching deleted data by month from database...")
    results = None

    try:
        with mwa_db.connection() as conn:
            cursor = conn.execute(
                """
                SELECT
                    date_part('year', date_trunc('day', deleted_timestamp)) as reporting_year
                    ,date_part('month', date_trunc('day', deleted_timestamp)) as reporting_month
                    ,COALESCE(SUM(size), 0) as deleted_bytes
                FROM data_files
                WHERE deleted_timestamp IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            )
            results = cursor.fetchall()
    except (psycopg.DatabaseError, psycopg.OperationalError) as error:
        logger.error(error)

    if results is None:
        return []

    return [
        {
            "reporting_year": int(row[0]),
            "reporting_month": int(row[1]),
            "deleted_bytes": int(row[2]),
        }
        for row in results
    ]


def fetch_file_type_stats_from_db(mwa_db: ConnectionPool) -> list[FileTypeStats]:
    """Fetch size statistics per data type from the MWA database.

    Excludes deleted files.

    Args:
        mwa_db: A psycopg connection pool for the MWA database.

    Returns:
        One FileTypeStats per data type. Raw correlator files are split by era.
        An empty list if the query failed.
    """
    logger.info("Fetching file type statistics from database...")
    stats = []

    try:
        with mwa_db.connection() as conn, conn.cursor() as cursor:
            for era_label, where_clause in build_filetype_slice_where_clauses("f.observation_num", "f.filetype"):
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
                    stats.append(
                        FileTypeStats(
                            label=format_file_type_label(int(row[0]), row[1], era_label),
                            file_count=int(row[2]),
                            min_bytes=int(row[3]),
                            mean_bytes=float(row[4]),
                            median_bytes=int(row[5]),
                            max_bytes=int(row[6]),
                            total_bytes=int(row[7]),
                        )
                    )
    except (psycopg.DatabaseError, psycopg.OperationalError) as error:
        logger.error(error)
        return []

    return stats
