import os
import pandas as pd
import logging

from .base_stream import Stream
from loaders.postgres_loader import PostgresLoader
from extractors.bitrix_extractor import BitrixAPIExtractor

logger = logging.getLogger(__name__)


class BitrixStream(Stream):
    def __init__(self, table):
        """
        Inicializa uma BitrixStream com um objeto DataTable.

        Args:
            table (DataTable): Objeto DataTable com as configurações da fonte
        """
        super().__init__(table)
        self.extractor = None
        self.loader = None
        self.ddl = None

    def set_extractor(self):
        """
        Configura o BitrixAPIExtractor para esta stream.
        """
        self.extractor = BitrixAPIExtractor(self.table)

    def extract_stream(self):
        """
        Extracts data from Bitrix using the configured extractor.
        
        
        Args:
            row: Row from the configuration table with source_name, extraction_strategy, 
                days_interval, and updated_at_column


        Args:
            row: Row from the configuration table with source_name, extraction_strategy, 
                days_interval, and updated_at_column

        Returns:
            DataFrame: The extracted data
        """
        logger.info("Extracting data from Bitrix API")
        if not self.extractor:
            self.set_extractor()
        return self.extractor.run()

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
        Carrega os dados na camada staging no banco de dados de destino.

        Args:
            records (pd.DataFrame): DataFrame com os registros a serem carregados
            chunksize (int, optional): Tamanho do chunk para carregamento em lotes
        """
        
        if not self.loader:
            raise ValueError("Loader not set. Call set_loader() first.")

        if not isinstance(records, pd.DataFrame):
            raise TypeError("Records must be a pandas DataFrame")

        logger.info(f"{__name__} Chamando load_data com raw_data.shape: {records.shape}")

        logger.info(
            f"Loading {len(records)} records into {self.table.origin}.{self.table.target_name}"
        )

        self.loader.load_data(df=records, chunksize=chunksize, mode="replace")
