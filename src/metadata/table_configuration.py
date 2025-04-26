import logging
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class TableConfiguration:
    def __init__(self, url, origin: str):
        self.db_url = url
        self.origin = origin
        
    def get_table_configuration(self):
        """Loads sync metadata information into a Pandas DataFrame."""
        query = f"""SELECT *
        FROM public.table_configuration ct
        where ct.origin = '{self.origin}'"""
        
        engine = create_engine(self.db_url, poolclass=QueuePool, pool_size=5, max_overflow=10)
        with engine.connect() as connection:
            try:
                result = connection.execute(text(query))
                return pd.DataFrame(result.fetchall(), columns=result.keys())
            except SQLAlchemyError as e:
                logger.error(f"Error loading table metadata: {e}")
                return None

    def update_table_configuration(self, id, source_name, last_successful_sync_at=None, last_sync_attempt_at=None):
        """
        Update the table metadata with sync information.
        
        Args:
            id: The ID of the table configuration record
            source_name: Source name for logging purposes
            last_successful_sync_at: Last successful sync timestamp
            last_sync_attempt_at: Last attempt timestamp
            
        Returns:
            bool: True on success, False on failure
        """
        query = """
            UPDATE public.table_configuration
            SET
                last_successful_sync_at = COALESCE(:last_successful_sync_at, last_successful_sync_at),
                last_sync_attempt_at = COALESCE(:last_sync_attempt_at, last_sync_attempt_at)
            WHERE id = :id
        """
        
        # Convert NumPy types to native Python types to avoid adapter errors
        if hasattr(id, 'item'):  # Check if it's a NumPy type with .item() method
            id = id.item()
        else:
            id = int(id) if id is not None else None
            
        params = {
            "id": id,
            "last_successful_sync_at": last_successful_sync_at,
            "last_sync_attempt_at": last_sync_attempt_at,
        }
        
        engine = create_engine(self.db_url, poolclass=QueuePool, pool_size=5, max_overflow=10)
        with engine.connect() as connection:
            try:
                connection.execute(text(query), params)
                logger.info(f"Successfully updated metadata for {self.origin}.{source_name}")
                return True
            except SQLAlchemyError as e:
                logger.error(f"Error updating metadata: {e}")
                return False
