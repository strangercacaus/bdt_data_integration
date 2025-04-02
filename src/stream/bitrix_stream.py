import os
import json
import pandas as pd
import logging
import requests

from .base_stream import Stream
from src.writers import DataWriter
from src.loader.postgres_loader import PostgresLoader
from src.extractor.bitrix_extractor import BitrixAPIExtractor
from src.utils import Utils, Schema

logger = logging.getLogger(__name__)

class BitrixStream(Stream):
    
    def __init__(self, source_name, config, **kwargs):
        """
        Initialize a BitrixStream with a source name and configuration.
        
        Args:
            source_name (str): Name of the source stream
            config (dict): Configuration dictionary
            **kwargs: Additional arguments
        """
        super().__init__(source_name, config, **kwargs)
        self.source = 'bitrix'
        self.output_name = kwargs.get('output_name', self.source_name)
        self._writer = None
    
    @property
    def writer(self):
        """
        Lazy-loaded writer property.
        
        Returns:
            DataWriter: The writer instance
        """
        if self._writer is None:
            self._writer = DataWriter(
                source=self.source,
                stream=self.source_name,
                compression=False,
                config=self.config
            )
        return self._writer
    
    def set_extractor(self, **kwargs):
        """
        Set up the BitrixAPIExtractor for this stream.
        
        Args:
            **kwargs: Arguments specific to the extractor
        """
        webhook_url = kwargs.get('webhook_url', None)
        client_id = kwargs.get('client_id', None)
        client_secret = kwargs.get('client_secret', None)
        entity = kwargs.get('entity', self.source_name)
        
        if webhook_url:
            self.extractor = BitrixAPIExtractor(
                webhook_url=webhook_url,
                entity=entity
            )
        elif client_id and client_secret:
            self.extractor = BitrixAPIExtractor(
                client_id=client_id,
                client_secret=client_secret,
                entity=entity
            )
        else:
            raise ValueError("Either webhook_url or client_id and client_secret must be provided")
    
    def set_schema(self):
        """Set up schema information for this stream."""
        self.schema = Schema()
    
    def extract_stream(self, **kwargs) -> None:
        """
        Extract data from Bitrix API and write it to the raw layer.
        
        Args:
            **kwargs: Additional arguments for extraction
        """
        try:
            if data := self.extractor.run():
                # Convert to DataFrame if it's a list
                if isinstance(data, list):
                    data = pd.DataFrame(data)

                # Write to raw layer
                raw_data_path = self.writer.get_output_file_path(target_layer='raw') + '.csv'

                os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

                if isinstance(data, pd.DataFrame):
                    data.to_csv(
                        raw_data_path,
                        sep=';',
                        index=False,
                        encoding='utf-8'
                    )
                else:
                    logging.error(f'Invalid data format: {type(data)}')
                    raise ValueError(f'Invalid data format: {type(data)}')
            else:
                logging.warning(f"No data returned from extractor for {self.source_name}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error during API request: {e}")
            raise
        except Exception as e:
            logging.error(f"Error extracting data: {e}")
            raise
    
    def transform_stream(self, **kwargs) -> None:
        """
        Transform the raw data and write it to the processing layer.
        
        Args:
            **kwargs: Additional arguments for transformation
        """
        # Lendo o arquivo na camada raw
        raw_data_path = self.writer.get_output_file_path(target_layer='raw') + '.csv'
        
        try:
            raw_data = pd.read_csv(
                raw_data_path,
                sep=';',
                encoding='utf-8'
            )
        except Exception as e:
            raise Exception(f'Error reading raw data: {e}') from e
            
        # Process specific transformations based on entity type
        transformers = {
            'leads': self._transform_leads,
            'deals': self._transform_deals,
            'contacts': self._transform_contacts
        }
        
        transform_func = transformers.get(self.source_name, lambda x: x)
        raw_data = transform_func(raw_data)
            
        # Write to processing layer
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        
        raw_data.to_csv(
            processed_data_path,
            sep=';',
            index=False,
            encoding='utf-8'
        )
    
    def stage_stream(self, rename_columns=False, **kwargs):
        """
        Process the transformed data and write it to the staging layer.
        
        Args:
            rename_columns (bool): Whether to rename columns using a mapping file
            **kwargs: Additional arguments for staging
        """
        # Lendo o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        
        processed_data = pd.read_csv(
            processed_data_path,
            sep=';',
            encoding='utf-8',
            dtype=str
        )
        
        if rename_columns:
            mapping_file_path = kwargs.get('mapping_file_path', None)
            if not mapping_file_path:
                raise Exception('Caminho do arquivo mapping não foi informado')
            try:
                with open(mapping_file_path, 'r') as file:
                    mapping = json.load(file)
                    
                processed_data = Utils.rename_columns(processed_data, mapping)
            except Exception as e:
                raise Exception(f'Erro ao ler o arquivo mapping: {e}') from e
        else:
            processed_data.columns = processed_data.columns.str.lower()
            
        # Custom column transformations
        date_columns = kwargs.get('date_columns', [])
        for col in date_columns:
            if col in processed_data.columns:
                processed_data[col] = pd.to_datetime(processed_data[col], errors='coerce')
                
        # Write to staging layer
        staged_data_path = self.writer.get_output_file_path(
            output_name=self.output_name,
            target_layer='staging'
        ) + '.csv'
        
        os.makedirs(os.path.dirname(staged_data_path), exist_ok=True)
        
        processed_data.to_csv(
            staged_data_path,
            sep=';',
            index=False,
            encoding='utf-8'
        )
    
    def set_loader(self, user, password, host, db_name, schema_file_type):
        """
        Set up the PostgresLoader for this stream.
        
        Args:
            user (str): Database username
            password (str): Database password
            host (str): Database host
            db_name (str): Database name
            schema_file_type (str): Type of schema file
        """
        schema_file_path = f'./config/{self.source}/{self.source_name}_schema.json'
        
        self.loader = PostgresLoader(
            user=user,
            password=password,
            host=host,
            db_name=db_name,
            schema_file_path=schema_file_path,
            schema_file_type=schema_file_type
        )
    
    def load_stream(self, target_schema, target_table, **kwargs):
        """
        Load the staged data into the target database.
        
        Args:
            target_schema (str): Name of the target schema
            target_table (str): Name of the target table
            **kwargs: Additional arguments for loading
        """
        mode = kwargs.get('mode', 'replace')
        
        staged_data_path = self.writer.get_output_file_path(
            output_name=self.output_name,
            target_layer='staging'
        ) + '.csv'
        
        staged_data = pd.read_csv(
            staged_data_path,
            sep=';',
            encoding='utf-8'
        )
        
        self.loader.load_data(
            df=staged_data,
            schema_name=target_schema,
            table_name=target_table or self.output_name,
            mode=mode
        )
        
    def _transform_leads(self, data):
        """Transform leads data"""
        return data
        
    def _transform_deals(self, data):
        """Transform deals data"""
        return data
        
    def _transform_contacts(self, data):
        """Transform contacts data"""
        return data 