import os
import json
import pandas as pd
import logging
import requests

from .base_stream import Stream
from writers import DataWriter
from loaders.postgres_loader import PostgresLoader
from extractors.bitrix_extractor import BitrixAPIExtractor
from utils import Utils

logger = logging.getLogger(__name__)  # This will use the module's name

class BitrixStream:
    def __init__(self, source_name, config, **kwargs):
        self.source = "bitrix"
        self.config = config
        self.source_name = source_name
        self.output_name = kwargs.get("output_name", self.source_name)

    @property
    def writer(self):
        return DataWriter(
            source=self.source,
            stream=self.source_name,
            compression=False,
            config=self.config,
        )

    def set_extractor(self, **kwargs):

        separator = kwargs.get("separator", ";")

        token = kwargs.get("token", None)

        bitrix_url = kwargs.get("bitrix_url", None)

        bitrix_user_id = kwargs.get("bitrix_user_id", None)

        if any(var is None for var in [token, bitrix_url, bitrix_user_id]):
            raise ValueError(
                "Variável obrigatória omitida em BitrixStream.set_extractor"
            )

        self.extractor = BitrixAPIExtractor(
            source=self.source,
            token=token,
            writer=self.writer,
            separator=separator,
            bitrix_url=bitrix_url,
            bitrix_user_id=bitrix_user_id,
        )

    def extract_stream(self, **kwargs) -> None:

        separator = kwargs.get("separator", ";")

        mode = kwargs.get("mode", "table")

        #start = kwargs.get("start", 0)

        extract_function = self.extractor.get_extract_function(mode)
        records = extract_function(self.source_name)

        raw_data_path = (
            self.writer.get_output_file_path(target_layer="raw", date=False) + ".csv"
        )

        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

        records.to_csv(raw_data_path, index=False, sep=separator, encoding="utf-8")

        return records

    def transform_stream(self, **kwargs) -> None:

        separator = kwargs.get(
            "separator", self.config.get("DEFAULT_CSV_SEPARATOR", ";")
        )
        base_dir = os.path.join(os.getcwd(), 'data')
        path = os.path.join(base_dir, 'raw', self.source)

        extension = ".csv"

        try:
            raw_data_path = Utils.get_latest_file(path, extension)

        except Exception:
            raw_data_path = path + extension

        if raw_data_path is None:
            raise Exception(f"No files found in the specified directory: {file_path}")

        records = pd.read_csv(raw_data_path, sep=separator, dtype=str)

        processed_data_path = (
            self.writer.get_output_file_path(target_layer="processing") + ".csv"
        )

        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        records.to_csv(
            processed_data_path, index=False, sep=separator, encoding="utf-8"
        )

    def stage_stream(self, **kwargs):

        separator = kwargs.get(
            "separator", self.config.get("DEFAULT_CSV_SEPARATOR", ";")
        )

        processed_data_path = (
            self.writer.get_output_file_path(target_layer="processing") + ".csv"
        )

        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        processed_data = pd.read_csv(
            processed_data_path, sep=separator, encoding="utf-8", dtype=str
        )

        processed_data_path = (
            self.writer.get_output_file_path(
                output_name=self.output_name, target_layer="staging"
            )
            + ".csv"
        )

        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        processed_data.to_csv(
            processed_data_path, index=False, sep=separator, encoding="utf-8"
        )

    def set_loader(self, engine, schema_file_type = None, schema_file_path = None):
        """
        Configura o PostgresLoader para esta stream.
        
        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine para a conexão com o banco de dados
            schema_file_path (str): Caminho para o arquivo de esquema para criar tabelas
            schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema
        """
        self.loader = PostgresLoader(engine, schema_file_path, schema_file_type)
        self.loader.schema = self.schema

    def load_stream(self, target_schema, target_table, **kwargs):

        mode = kwargs.get("mode", "replace")

        separator = kwargs.get(
            "separator", self.config.get("DEFAULT_CSV_SEPARATOR", ";")
        )

        staged_data_path = (
            self.writer.get_output_file_path(
                output_name=self.output_name, target_layer="staging"
            )
            + ".csv"
        )

        staged_data = pd.read_csv(
            staged_data_path, sep=separator, encoding="utf-8", dtype=str
        )
        
        logger.debug(f'Class Schema: {self.schema}')

        self.loader.load_data(
            df=staged_data,
            target_table=target_table,
            target_schema=target_schema,
            mode=mode,
            schema=self.schema,
        )
