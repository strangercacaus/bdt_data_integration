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


class BitrixStream(Stream):
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

        # start = kwargs.get("start", 0)

        records = self.extractor.run(mode, self.source_name)

        raw_data_path = (
            self.writer.get_output_file_path(target_layer="raw", date=False) + ".csv"
        )

        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

        records.to_csv(raw_data_path, index=False, sep=separator, encoding="utf-8")

        return records
    def set_loader(self, engine, schema_file_type=None, schema_file_path=None):
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

        raw_data_path = (
            self.writer.get_output_file_path(
                output_name=self.output_name, target_layer="raw"
            )
            + ".csv"
        )

        raw_data = pd.read_csv(
            raw_data_path, sep=separator, encoding="utf-8", dtype=str
        )

        logger.info(f"Chamando load_data com raw_data.shape: {raw_data.shape}")

        self.loader.load_data(
            df=raw_data,
            target_table=target_table,
            target_schema=target_schema,
            mode=mode,
            schema=self.schema,
        )
