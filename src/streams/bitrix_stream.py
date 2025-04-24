import os
import pandas as pd
import logging

from .base_stream import Stream
from writers import DataWriter
from loaders.postgres_loader import PostgresLoader
from extractors.bitrix_extractor import BitrixAPIExtractor

logger = logging.getLogger(__name__)  # This will use the module's name


class BitrixStream(Stream):
    def __init__(self, source_name, config, **kwargs):
        """
        Inicializa uma BitrixStream com um nome de fonte e uma configuração.

        Args:
            source_name (str): Nome da tabela fonte da stream
            config (dict): Dicionário de configuração
            **kwargs: Argumentos adicionais
        """
        super().__init__(source_name, config, **kwargs)
        self.source = "bitrix"
        self.source_name = source_name
        self.output_name = kwargs.get("output_name", self.source_name)
        self.writer = DataWriter(
            source=self.source,
            stream=self.source_name,
            compression=False,
            config=self.config,
        )

    def set_extractor(self, **kwargs):
        """
        Configura o BitrixAPIExtractor para esta stream.

        Args:
            database_id (str): ID do banco de dados Notion
            token (str): Token da API Notion
        """

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

    def extract_stream(self, mode) -> None:

        return self.extractor.run(mode, self.source_name)

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

    def load_stream(self, records, target_schema, target_table, **kwargs):
        """
        Carrega os dados na camada staging no banco de dados de destino.

        Args:
            target_schema (str): Nome do esquema de destino
            **kwargs: Argumentos adicionais para o carregamento
        """

        logger.info(f"Chamando load_data com raw_data.shape: {records.shape}")

        self.loader.load_data(
            df=records,
            target_table=target_table,
            target_schema=target_schema,
            mode="replace",
        )
