import logging

import psycopg2
import psycopg2.errors
import psycopg2.pool

from core import MEDIAN_QUANTILE, FileTypeStats, build_filetype_slice_where_clauses, format_file_type_label

logger = logging.getLogger("archive_stats")


def get_threaded_connection_pool(host, user, database, password, port) -> psycopg2.pool.ThreadedConnectionPool:
    """Create a threaded connection pool for the MWA database.

    Args:
        db_config: A dict with keys: host, port, dbname, user, password
    Returns:
        A psycopg2.pool.ThreadedConnectionPool instance.
    """
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=2,
        host=host,
        user=user,
        database=database,
        password=password,
        port=port,
    )


def fetch_deleted_data_by_month_from_db(mwa_db: psycopg2.pool.ThreadedConnectionPool):
    """Fetch all deleted-data monthly aggregates from the database (no date filter).

    Returns a list of dicts with keys: reporting_year, reporting_month, deleted_bytes
    """
    logger.info("Fetching deleted data by month from database...")
    conn = None
    results = None

    try:
        conn = mwa_db.getconn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                date_part('year', date_trunc('day', deleted_timestamp)) as reporting_year
                ,date_part('month', date_trunc('day', deleted_timestamp)) as reporting_month
                ,COALESCE(SUM(size), 0) as deleted_bytes
            FROM data_files
            WHERE deleted_timestamp IS NOT NULL AND deleted_timestamp >= '2013-07-01'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        )
        results = cursor.fetchall()
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            mwa_db.putconn(conn)

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


def fetch_file_type_stats_from_db(mwa_db: psycopg2.pool.ThreadedConnectionPool) -> list[FileTypeStats]:
    """Fetch size statistics per data type from the MWA database.

    Excludes deleted files.

    Args:
        mwa_db: A psycopg2 connection pool for the MWA database.

    Returns:
        One FileTypeStats per data type. Raw correlator files are split by era.
        An empty list if the query failed.
    """
    logger.info("Fetching file type statistics from database...")
    stats = []

    try:
        conn = mwa_db.getconn()
        with conn.cursor() as cursor:
            for era_label, where_clause in build_filetype_slice_where_clauses("f.observation_num", "f.filetype"):
                logger.info(f"...Fetching statistics for era: {era_label}")
                cursor.execute(
                    f"""
                    SELECT
                        f.filetype
                        ,t.description
                        ,COUNT(*)
                        ,MIN(f.size)
                        ,AVG(f.size)
                        ,percentile_disc({MEDIAN_QUANTILE}) WITHIN GROUP (ORDER BY f.size)
                        ,MAX(f.size)
                        ,SUM(f.size)
                    FROM data_files f
                    LEFT JOIN data_file_types t ON t.type = f.filetype
                    WHERE {where_clause} AND remote_archived=True AND deleted_timestamp IS NULL
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
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
        logger.error(error)
        return []
    finally:
        if conn is not None:
            mwa_db.putconn(conn)

    return stats
