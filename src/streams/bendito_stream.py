import os
import json
import pandas as pd
import logging

from .base_stream import Stream
from writers import DataWriter
from loaders.postgres_loader import PostgresLoader
from extractors.bendito_extractor import BenditoAPIExtractor
from utils import Utils

logger = logging.getLogger(__name__)


class BenditoStream(Stream):

    def __init__(self, source_name, config, **kwargs):
        """
        Initialize a BenditoStream with a source name and configuration.

        Args:
            source_name (str): Name of the source stream
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

        separator = kwargs.get(
            'separator',
            ';'
            )

        order_col = kwargs.get(
            'order_col',
            1
            )
        
        if custom_query:
            query = custom_query.strip().rstrip(';')
        else:
            query = f'select * from "{self.source_name}" order by {order_col} asc'

        records = self.extractor.run(
            query = query,
            separator = separator,
            page_size = page_size)
            
        raw_data_path = self.writer.get_output_file_path(
            target_layer = 'raw',
            date = False
            ) + '.csv'

        os.makedirs(
            os.path.dirname(raw_data_path),
            exist_ok = True
            )

        records.to_csv(
            raw_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )

        return records

    def set_loader(self, engine, schema_file_path, schema_file_type):
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
        self.loader = PostgresLoader(engine, schema_file_path, schema_file_type)

    def load_stream(self, target_schema, **kwargs):
        """
        Load the staged data into the target database.

        Args:
            target_schema (str): Name of the target schema
            **kwargs: Additional arguments for loading
        """
        mode = kwargs.get("mode", "replace")
        separator = kwargs.get("separator", self.separator)

        raw_data_path = (
            self.writer.get_output_file_path(
                output_name=self.output_name, target_layer="raw"
            )
            + ".csv"
        )

        raw_data = pd.read_csv(
            raw_data_path, sep=separator, encoding="utf-8", dtype=str
        )

        self.loader.load_data(
            df=raw_data,
            target_schema=target_schema,
            target_table=self.output_name,
            mode=mode,
        )
