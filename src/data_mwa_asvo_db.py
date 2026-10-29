"""Database code for the MWA ASVO database."""

import logging

import psycopg
from psycopg_pool import ConnectionPool

from core import StagingTime, to_awst

logger = logging.getLogger("archive_stats")

# For each job, take the first 'staging' state and the first 'staged' state at or after it. Jobs that went
# staging -> staged -> staging -> staged would otherwise give extra pairs with wrong (too long) elapsed times.
STAGING_TIMES_SQL = """
    SELECT
        j.created AS staging_date_time
        ,EXTRACT(EPOCH FROM (MIN(s2.change_datetime) - fs.staging_start)) AS staging_elapsed_secs
    FROM (
        SELECT job_id, MIN(change_datetime) AS staging_start
        FROM public.job_state_history
        WHERE job_state = 'staging'
        GROUP BY job_id
    ) fs
    INNER JOIN public.job_state_history s2
        ON s2.job_id = fs.job_id AND s2.job_state = 'staged' AND s2.change_datetime >= fs.staging_start
    INNER JOIN public.jobs j ON j.id = fs.job_id
    GROUP BY j.id, j.created, fs.staging_start
    ORDER BY 1
"""


def fetch_staging_times_from_db(mwa_asvo_db: ConnectionPool) -> list[StagingTime]:
    """Fetch the staging time of every MWA ASVO job that has been staged.

    Args:
        mwa_asvo_db: The MWA ASVO database connection pool.

    Returns:
        One entry per job, oldest first. The creation time of the job is in AWST.

    Raises:
        psycopg.DatabaseError: If the query fails. This is not caught because empty results would make the
            reported staging statistics silently wrong.
        psycopg.OperationalError: If the database connection fails.
    """
    logger.info("Fetching staging times from the MWA ASVO database...")

    try:
        with mwa_asvo_db.connection() as conn:
            results = conn.execute(STAGING_TIMES_SQL).fetchall()
    except psycopg.DatabaseError, psycopg.OperationalError:
        logger.exception("Failed to fetch staging times from the MWA ASVO database.")
        raise

    return [StagingTime(created_awst=to_awst(row[0]), elapsed_secs=float(row[1])) for row in results]
