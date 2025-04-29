import os
import pandas as pd
import logging

from .base_stream import Stream
from loaders.postgres_loader import PostgresLoader
from extractors.bendito_extractor import BenditoAPIExtractor

logger = logging.getLogger(__name__)


class BenditoStream(Stream):

    def __init__(self, table):
        """
        Initialize a BenditoStream with a DataTable object.

        Args:
            table (DataTable): DataTable object with source configuration
        """
        super().__init__(table)
        self.extractor = None
        self.loader = None

    def set_extractor(self):
        """
        Set up the BenditoAPIExtractor for this stream.
        """
        self.extractor = BenditoAPIExtractor(self.table)
        logger.info(f"BenditoAPIExtractor set for {self.table.source_name}")

    def extract_stream(self, page_size: int = 1000):
        """
        Extract data from Bendito API and return as DataFrame.

        Args:
            page_size (int): Page size for data extraction

        Returns:
            DataFrame: The extracted data
        """
        logger.info(f"Extracting data from Bendito API for {self.table.source_name}")
        if not self.extractor:
            logger.info("Extractor not set, setting it now")
            self.set_extractor()

        return self.extractor.run(page_size=page_size)

    def set_table_definition(self, table_definition=None):
        """
        Set the table definition for this stream.

        Args:
            table_definition (str, optional): SQL DDL statement for table creation
        """
        if not table_definition:
            table_definition = self.table.schemaless_ddl

        self.table_definition = table_definition
        logger.info(f"Table definition set for {self.table.source_name}")

    def set_loader(self, engine):
        """
        Set up the PostgreSQLLoader for this stream.

        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection
        """
        self.loader = PostgresLoader(engine, self.table)
        self.loader.table_definition = self.table.schemaless_ddl
        logger.info(f"PostgreSQLLoader set for {self.table.source_name}")

    def load_stream(self, records, chunksize=None):
        """
        Load the data into the target database.

        Args:
            records (pd.DataFrame): DataFrame with records to be loaded
            chunksize (int, optional): Chunk size for batch loading
        """
        if not self.loader:
            raise ValueError("Loader not set. Call set_loader() first.")

        if not isinstance(records, pd.DataFrame):
            raise TypeError("Records must be a pandas DataFrame")

        logger.info(
            f"Loading {len(records)} records into {self.table.origin}.{self.table.raw_model_name}"
        )

        self.loader.load_data(df=records, chunksize=chunksize, mode="replace")
