import logging
import sqlite3
import time
from sqlalchemy.exc import OperationalError

logger = logging.getLogger()

def retry_transaction(max_retries=10):
    """
    Decorator to wrap functions which writes to the sqlite database to prevent "database is locked" error with multithreading.
    """
    def wrapper(fn):
        def inner(session, *args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return fn(session, *args, **kwargs)
                except (OperationalError, sqlite3.OperationalError) as e:
                    session.rollback()
                    if attempt == max_retries - 1:
                        logger.error("Database locked, giving up. (%s/%s). Error: %s", attempt, max_retries, e)
                        logger.error("Please run ./main.py db repair to remove stale entries!")
                        raise
                    logger.warning(
                        "Database locked, waiting 5 seconds for next retry (%s/%s). Error: %s", attempt, max_retries, e)
                    time.sleep(5)
            return None

        return inner
    return wrapper
