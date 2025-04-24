import os
import pandas as pd
import logging

from .base_stream import Stream
from writers import DataWriter
from loaders.postgres_loader import PostgresLoader
from extractors.bendito_extractor import BenditoAPIExtractor

logger = logging.getLogger(__name__)


class BenditoStream(Stream):

    def __init__(self, source_name, config, **kwargs):
        """
        Initialize a BenditoStream with a source name and configuration.

        Args:
            source_name (str): Nome da tabela fonte da stream
            config (dict): Configuration dictionary
            **kwargs: Additional arguments
        """
        super().__init__(source_name, config, **kwargs)
        self.source = "bendito"
        self.source_name = source_name
        self.output_name = kwargs.get("output_name", self.source_name)
        self.writer = DataWriter(
            source=self.source,
            stream=self.source_name,
            compression=False,
            config=self.config,
        )

    def set_extractor(self, token, separator=";"):
        """
        Set up the BenditoAPIExtractor for this stream.

        Args:
            token (str): Bendito API token
            separator (str): CSV separator
        """
        self.extractor = BenditoAPIExtractor(token=token)
        self.separator = separator

    def extract_stream(self, custom_query=None, page_size=500, **kwargs) -> None:

        separator = kwargs.get("separator", ";")

        order_col = kwargs.get("order_col", 1)

        if custom_query:
            query = custom_query.strip().rstrip(";")
        else:
            query = f'select * from "{self.source_name}" order by {order_col} asc'

        return self.extractor.run(query=query, separator=separator, page_size=page_size)

    def set_table_definition(self, ddl):
        self.table_definition = ddl

    def set_loader(self, engine):
        """
        Set up the PostgresLoader for this stream.

        Args:
            user (str): Database username
            password (str): Database password
            host (str): Database host
            db_name (str): Database name
            schema_file_path (str): Path to schema file
            schema_file_type (str): Type of schema file
        """
        self.loader = PostgresLoader(engine)
        self.loader.table_definition = self.table_definition

    def load_stream(self, records, target_schema, target_table, **kwargs):
        """
        Load the staged data into the target database.

        Args:
            target_schema (str): Name of the target schema
            **kwargs: Additional arguments for loading
        """

        logger.info(f"Chamando load_data com raw_data.shape: {records.shape}")

        self.loader.load_data(
            df=records,
            target_table=target_table,
            target_schema=target_schema,
            mode="replace",
        )
