import os
import pandas as pd
import logging

from .base_stream import Stream
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

    def set_extractor(self):
        """
        Configura o BitrixAPIExtractor para esta stream.

        Args:
            database_id (str): ID do banco de dados Notion
            token (str): Token da API Notion
        """
        self.extractor = BitrixAPIExtractor()

    def extract_stream(self, row):
        """
        Extracts data from Bitrix using the configured extractor.
        
        Args:
            row: Row from the configuration table with source_name, extraction_strategy, 
                days_interval, and updated_at_column

        Returns:
            DataFrame: The extracted data
        """
        return self.extractor.run(row.source_identifier, row.extraction_strategy, row.days_interval, row.updated_at_property)

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

    def load_stream(self, records, target_schema, target_table, chunksize=None):
        """
        Carrega os dados na camada staging no banco de dados de destino.

        Args:
            records (pd.DataFrame): DataFrame com os registros a serem carregados
            target_schema (str): Nome do esquema de destino
            target_table (str): Nome da tabela de destino
            chunksize (int, optional): Tamanho do chunk para carregamento em lotes
        """

        logger.info(f"Chamando load_data com raw_data.shape: {records.shape}")

        self.loader.load_data(
            df=records,
            target_table=target_table,
            target_schema=target_schema,
            mode="replace",
            chunksize=chunksize
        )
