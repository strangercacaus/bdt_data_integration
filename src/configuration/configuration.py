import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from src.util import Utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MetadataHandler:
    def __init__(self, engine, source: str):
        self.engine = engine
        self.source = source
        
    def _load_table_meta(self):
        """Loads metadata table information into a Pandas DataFrame."""
        query = f"""SELECT ct.source, ct.table_name, ct.target_name, ct.active, ct.last_successful_sync_at, ct.last_sync_attempt_at, ct.vars 
        FROM configuration.table ct where ct.source = '{self.source}';"""
        
        with self.engine.connect() as connection:
            try:
                result = connection.execute(query)
                # Convert result to a Pandas DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
            except SQLAlchemyError as e:
                logger.error(f"Error loading table metadata: {e}")
                return None

    def update_table_meta(self, table, active=None, last_successful_sync_at=None, last_sync_attempt_at=None):
        """Updates metadata in the configuration.table."""
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
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.engine.connect() as connection:
                    connection.execute(text(query), params)
                break
            except OperationalError as e:
                logger.warning(f"Retry {attempt + 1}/{max_retries} failed: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            except SQLAlchemyError as e:
                logger.error(f"Error: {e}")
                break
