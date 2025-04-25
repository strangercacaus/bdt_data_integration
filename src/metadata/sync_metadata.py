import logging
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SyncMetadataHandler:
    def __init__(self, engine, source: str):
        self.engine = engine
        self.source = source
        
    def _load_sync_meta(self):
        """Loads metadata table information into a Pandas DataFrame."""
        query = f"""SELECT ct.source, ct.table_name, ct.target_name, ct.active, ct.last_successful_sync_at, ct.last_sync_attempt_at, ct.vars 
        FROM configuration.table ct where ct.source = '{self.source}';"""
        
        with self.engine.connect() as connection:
            try:
                result = connection.execute(text(query))
                # Convert result to a Pandas DataFrame
                return pd.DataFrame(result.fetchall(), columns=result.keys())
            except SQLAlchemyError as e:
                logger.error(f"Error loading table metadata: {e}")
                return None

    def update_table_meta(self, table, active=None, last_successful_sync_at=None, last_sync_attempt_at=None):
        query = """
            UPDATE configuration.table
            SET
                active = COALESCE(:active, active),
                last_successful_sync_at = COALESCE(:last_successful_sync_at, last_successful_sync_at),
                last_sync_attempt_at = COALESCE(:last_sync_attempt_at, last_sync_attempt_at)
            WHERE source = :source AND table_name = :table_name
        """
        
        params = {
            "source": self.source,
            "table_name": table,
            "active": active,
            "last_successful_sync_at": last_successful_sync_at,
            "last_sync_attempt_at": last_sync_attempt_at,
        }
        
        max_retries = 5
        retry_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
                # Create a fresh connection for each attempt
                with self.engine.connect() as connection:
                    connection.execute(text(query), params)
                logger.info(f"Successfully updated metadata for {self.source}.{table}")
                return True  # Success - exit the retry loop
            except Exception as e:
                if "SSL connection has been closed unexpectedly" in str(e) or "connection has been closed" in str(e):
                    logger.warning(f"Connection lost on attempt {attempt+1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:  # Don't sleep on the last iteration
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30 seconds
                        # Recreate engine if needed
                        if attempt >= 2:  # After a couple retries, try recreating the engine
                            logger.info("Recreating database engine...")
                            self.engine = create_engine(self.engine.url)
                else:
                    logger.error(f"Database error on attempt {attempt+1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 30)
            except SQLAlchemyError as e:
                logger.error(f"SQLAlchemy error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 10)  # Shorter backoff for other errors
            except Exception as e:
                logger.error(f"Unexpected error updating metadata: {e}")
                break  # Exit immediately for unexpected errors
                
        logger.error(f"Failed to update metadata for {self.source}.{table} after {max_retries} attempts")
        return False  # Failed all retry attempts
