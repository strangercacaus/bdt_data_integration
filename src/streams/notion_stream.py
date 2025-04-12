import os
import json
import pandas as pd
import logging

# Adicionando diretório dos módulos personalizados ao PATH

from .base_stream import Stream
from writers import DataWriter
from loaders.postgres_loader import PostgresLoader
from extractors.notion_extractor import NotionDatabaseAPIExtractor
from utils import Utils
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class NotionStream(Stream):

    def __init__(self, source_name, config, **kwargs):
        """
        Inicializa uma NotionStream com um nome de fonte e uma configuração.

        Args:
            source_name (str): Nome da fonte da stream
            config (dict): Dicionário de configuração
            **kwargs: Argumentos adicionais
        """
        super().__init__(source_name, config, **kwargs)
        self.source = "notion"
        self.output_name = kwargs.get("output_name", self.source_name)
        self.writer = DataWriter(
            source=self.source,
            stream=self.source_name,
            compression=False,
            config=self.config,
        )

    def set_extractor(self, database_id, token):
        """
        Configura o NotionDatabaseAPIExtractor para esta stream.

        Args:
            database_id (str): ID do banco de dados Notion
            token (str): Token da API Notion
        """
        self.extractor = NotionDatabaseAPIExtractor(
            token=token, database_id=database_id
        )

    def extract_stream(self, **kwargs) -> None:
        """
        Extrai dados da API Notion e escreve para a camada raw.
        """

        separator = kwargs.get("separator", ";")

        records = self.extractor.run()

        raw_data_path = (
            self.writer.get_output_file_path(target_layer="raw", date=False) + ".csv"
        )

        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

        records.to_csv(raw_data_path, index=False, sep=separator, encoding="utf-8")

        return records

    def set_loader(self, engine):
        """
        Configura o PostgresLoader para esta stream.

        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine para a conexão com o banco de dados
            schema_file_path (str): Caminho para o arquivo de esquema para criar tabelas
            schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema
        """
        self.loader = PostgresLoader(engine)
        self.loader.schema = self.schema

    def load_stream(self, target_schema, target_table, **kwargs):
        """
        Carrega os dados na camada staging no banco de dados de destino.

        Args:
            target_schema (str): Nome do esquema de destino
            **kwargs: Argumentos adicionais para o carregamento
        """
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

        logger.info(f"Chamando load_data com staged_data.shape: {raw_data.shape}")

        self.loader.load_data(
            df=raw_data,
            target_table=target_table,
            target_schema=target_schema,
            mode=mode,
            schema=self.schema,
        )
