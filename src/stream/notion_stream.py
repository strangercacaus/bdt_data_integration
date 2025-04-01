import os
import json
import pandas as pd
import logging

# Adicionando diretório dos módulos personalizados ao PATH

from .base_stream import Stream
from src.writers import DataWriter
from src.loader.postgres_loader import PostgresLoader
from src.transformers import NotionTransformer
from src.extractor.notion_extractor import NotionDatabaseAPIExtractor
from src.utils import Utils

logger = logging.getLogger(__name__)

class NotionStream(Stream):
    
    def __init__(self, source_name, config, **kwargs):
        """
        Initialize a NotionStream with a source name and configuration.
        
        Args:
            source_name (str): Name of the source stream
            config (dict): Configuration dictionary
            **kwargs: Additional arguments
        """
        super().__init__(source_name, config, **kwargs)
        self.source = 'notion'
        self.output_name = kwargs.get('output_name', self.source_name)
        self.writer = DataWriter(
            source=self.source,
            stream=self.source_name,
            compression=False,
            config=self.config
        )
    
    def set_extractor(self, database_id, token):
        """
        Set up the NotionDatabaseAPIExtractor for this stream.
        
        Args:
            database_id (str): Notion database ID
            token (str): Notion API token
        """
        self.extractor = NotionDatabaseAPIExtractor(
            token=token,
            database_id=database_id
        )
    
    def extract_stream(self) -> None:
        """
        Extract data from Notion API and write it to the raw layer.
        """
        records = self.extractor.run()

        self.writer.dump_records(
            records,
            target_layer='raw',
            date=False
        )

    def transform_stream(self, entity: str = None, **kwargs) -> None:
        """
        Transform the raw data and write it to the processing layer.
        
        Args:
            entity (str, optional): Type of entity to transform ('pages' or 'users')
            **kwargs: Additional arguments for transformation
        """
        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR', ';')
        )

        transformer = NotionTransformer()

        source_name = kwargs.get('source_stream', self.source_name)

        path = f'/work/data/raw/{self.source}/{source_name}'

        extension = '.txt'
        
        if self.writer.compression == True:
            extension += '.gz'
    
        try:
            raw_data_path = Utils.get_latest_file(path, extension)
        except Exception:
            raw_data_path = path + extension

        if raw_data_path is None:
            raise Exception(f'{__name__}: raw_data_path é vazio.')

        try:
            records = Utils.read_records(raw_data_path)
        except Exception as e:
            raise Exception(
                f'No files found in the specified directory: {raw_data_path} ({e})'
            ) from e

        if entity == 'pages':
            # Extrair propriedades dos registros
            processed_data = transformer.extract_pages_from_records(records)

            # Transformar colunas de lista em strings separadas por vírgulas
            transformer.process_list_columns(processed_data)

            if self.source_name == 'universal_task_database':
                # Remover o início do nome das etapas
                processed_data['Etapa'] = processed_data['Etapa'].str[4:]

                # Atualizar a coluna Task Interval com o atributo 'start' do objeto
                processed_data['Duração da Tarefa'] = processed_data['Duração da Tarefa'].apply(
                    lambda x: x['start'] if isinstance(x, dict) and 'start' in x else None
                )

        elif entity == 'users':
            processed_data = transformer._extract_users_list(records)

        # Gravando o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(
            target_layer='processing'
        ) + '.csv'

        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        processed_data.to_csv(
            processed_data_path,
            sep=separator,
            index=False,
            encoding='utf-8'
        )
    
    def stage_stream(self, rename_columns:bool = False, **kwargs):
        """
        Process the transformed data and write it to the staging layer.
        
        Args:
            rename_columns (bool): Whether to rename columns using a mapping file
            **kwargs: Additional arguments for staging
        """
        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR', ';')
        )

        # Lendo o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(
            target_layer='processing'
        ) + '.csv'

        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        processed_data = pd.read_csv(
            processed_data_path,
            sep=separator,
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

        staged_data_path = self.writer.get_output_file_path(
            output_name=self.output_name,
            target_layer='staging'
        ) + '.csv'

        os.makedirs(os.path.dirname(staged_data_path), exist_ok=True)

        processed_data.to_csv(
            staged_data_path,
            sep=separator,
            index=False,
            encoding='utf-8'
        )
    
    def set_loader(self, user, password, host, db_name, schema_file_path, schema_file_type):
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
        self.loader = PostgresLoader(
            user=user,
            password=password,
            host=host,
            db_name=db_name,
            schema_file_path=schema_file_path,
            schema_file_type='template'
        )
    
    def load_stream(self, target_schema, **kwargs):
        """
        Load the staged data into the target database.
        
        Args:
            target_schema (str): Name of the target schema
            **kwargs: Additional arguments for loading
        """
        mode = kwargs.get('mode', 'replace')
        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR', ';')
        )
        schema_file_path = kwargs.get('schema_file_path', None)

        staged_data_path = self.writer.get_output_file_path(
            output_name=self.output_name,
            target_layer='staging'
        ) + '.csv'

        staged_data = pd.read_csv(
            staged_data_path,
            sep=separator,
            encoding='utf-8'
        )

        self.loader.load_data(
            df=staged_data,
            schema_name=target_schema,
            table_name=self.output_name,
            mode=mode
        ) 