import os
import pandas as pd
import logging

from .base_stream import Stream
from loaders.postgres_loader import PostgresLoader
from extractors.notion_extractor import NotionDatabaseAPIExtractor

logger = logging.getLogger(__name__)


class NotionStream(Stream):

    def __init__(self, source_name, config, **kwargs):
        """
        Inicializa uma NotionStream com um nome de fonte e uma configuração.

        Args:
            source_name (str): Nome da tabela fonte da stream
            config (dict): Dicionário de configuração
            **kwargs: Argumentos adicionais
        """
        super().__init__(source_name, config, **kwargs)
        self.source = "notion"
        self.output_name = kwargs.get("output_name", self.source_name)

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

    def extract_stream(self, days: int = 0, **kwargs) -> None:
        """
        Extrai dados da API Notion e escreve para a camada raw.
        """

        return self.extractor.run(days=days)

    def set_table_definition(self, ddl):
        self.table_definition = ddl

    def set_loader(self, engine):
        """
        Configura o PostgresLoader para esta stream.

        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine para a conexão com o banco de dados
            schema_file_path (str): Caminho para o arquivo de esquema para criar tabelas
            schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema
        """
        self.loader = PostgresLoader(engine)
        self.loader.table_definition = self.table_definition

    def load_stream(self, records, target_schema: str, target_table: str, **kwargs):
        """
        Carrega os dados na camada staging no banco de dados de destino.

        Args:
            target_schema (str): Nome do esquema de destino
            **kwargs: Argumentos adicionais para o carregamento
        """

        logger.info(f"Chamando load_data com staged_data.shape: {records.shape}")

        self.loader.load_data(
            df=records,
            target_table=target_table,
            target_schema=target_schema,
            mode="replace",
        )
