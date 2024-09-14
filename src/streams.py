# Importação de módulos.
import os
import sys
import json
import pandas as pd
import logging

# Adicionando diretório dos módulos personalizados ao PATH
sys.path.append(os.path.abspath('bdt_data_integration'))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Importando módulos personalizadso
from src.writers import DataWriter
from src.loaders import PostgresLoader
from src.transformers import NotionTransformer
from src.extractors import NotionDatabaseAPIExtractor, BenditoAPIExtractor
from src.utils import Utils, WebhookNotifier, DiscordNotifier

class NotionStream():
    def __init__(self, source_name, config, **kwargs):
        self.source = 'notion'
        self.config = config
        self.source_name = source_name
        self.output_name = kwargs.get('output_name',self.source_name)
        self.writer = DataWriter(source=self.source, stream=self.source_name, compression = True, config= self.config)
    
    # def extract_database(self, database_id, token) -> None:
    #     extractor = NotionDatabaseAPIExtractor(token = token, database_id = database_id)
    #     records = extractor.run()
    #     self.writer.dump_records(records, target_layer = 'raw', date=True)
    
    def extract_stream(self, database_id, token) -> None:
        extractor = NotionDatabaseAPIExtractor(token = token, database_id = database_id)
        records = extractor.run()
        self.writer.dump_records(records, target_layer = 'raw', date=True)

    def transform_stream(self, entity: str = ['pages','users'], **kwargs) -> None:
        transformer = NotionTransformer()
        source_name = kwargs.get('source_stream', self.source_name)
        file_path = Utils.get_latest_file(f'/work/data/raw/{self.source}/{source_name}', '.txt.gz')
        if file_path:
            records = Utils.read_records(file_path)
        else:
            raise Exception(f'No files found in the specified directory: {file_path}')
        
        if entity == 'pages':
            # Extrair propriedades dos registros
            processed_data = transformer.extract_pages_from_records(records)

            # Transformar colunas de lista em strings separadas por vírgulas
            transformer.process_list_columns(processed_data)

            # Remover o início do nome das etapas
            processed_data['Etapa'] = processed_data['Etapa'].str[4:]

            # Atualizar a coluna Task Interval com o atributo 'start' do objeto
            processed_data['Task Interval']  = processed_data['Task Interval'].apply(lambda x: x['start'] if isinstance(x, dict) and 'start' in x else None)
        elif entity == 'users':
            processed_data = transformer._extract_users_list(records)

        # Gravando o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data.to_csv(processed_data_path, index=False)
    
    def stage_stream(self):
        # Lendo o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data = pd.read_csv(processed_data_path)

        # Atualizando o nome das colunas conforme o mapping
        
        try:
            mapping_file_path = f'/work/schema/{self.output_name}.json'
            with open(mapping_file_path, 'r') as file:
                mapping = json.load(file)
        except:
            mapping_file_path = f'/datasets/_deepnote_work/schema/{self.output_name}.json'
            with open(mapping_file_path, 'r') as file:
                mapping = json.load(file)
        processed_data = Utils.rename_columns(processed_data, mapping)
        output = self.writer.get_output_file_path(output_name = self.output_name,target_layer='staging') + '.csv'
        os.makedirs(os.path.dirname(output), exist_ok=True)
        processed_data.to_csv(output, index=False)
    
    def load_stream(self, user, password, host, db_name, schema, mode = 'replace', **kwargs):
        loader = PostgresLoader(user=user, password=password, host=host, db_name=db_name)
        path_to_schema_file = kwargs.get('path_to_schema_file',None)
        staged_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        staged_data = pd.read_csv(staged_data_path)
        loader.load_data(dataframe=staged_data, target_table=self.output_name, mode=mode, target_schema=schema, path_to_schema_file=path_to_schema_file)

class BenditoStream():
    def __init__(self, source_name,  config, **kwargs):
        self.source = 'bendito'
        self.config = config
        self.source_name = source_name 
        self.output_name = kwargs.get('output_name',self.source_name)
        self.writer = DataWriter(source=self.source, stream=self.source_name, compression = False, config= self.config)

    def extract_stream(self, token, source_name=None, custom_query=None, page_size=500, **kwargs) -> None:
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR'))
        if not custom_query:
            query = f"select * from {self.source_name}"
        else:
            query = custom_query.strip().rstrip(';')
        extractor = BenditoAPIExtractor(source = self.source, query = query, token = token, page_size = page_size, separator = separator, writer = self.writer)
        records = extractor.run(query = query, separator = separator, page_size = page_size)
        raw_data_path = self.writer.get_output_file_path(target_layer='raw', date=True) + '.csv'
        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)
        records.to_csv(raw_data_path, index=False, sep=separator)

    def transform_stream(self, **kwargs) -> None:
        # Lendo o arquivo na camada raw
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        file_path = Utils.get_latest_file(f'/work/data/raw/{self.source}/{self.source_name}', '.csv')
        if file_path:
                records = pd.read_csv(file_path,sep=separator)
        else:
            raise Exception(f'No files found in the specified directory: {file_path}')    

        ## .0000000000000000000000000. ##
        ## .0000000000000000000000000. ##
        ## .00 Transformações aqui 00. ##
        ## .0000000000000000000000000. ##
        ## .0000000000000000000000000. ##

        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        records.to_csv(processed_data_path, index=False, sep=separator)
    
    def stage_stream(self, rename_columns=False, **kwargs):

        # Lendo o arquivo na camada processing
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data = pd.read_csv(processed_data_path, sep=separator)

        # Atualizando o nome das colunas conforme o mapping
        if rename_columns:
            path_to_mapping_file = kwargs.get('path_to_mapping_file',None)
            if not path_to_mapping_file:
                raise Exception('Caminho do arquivo mapping não foi informado')
            try:
                with open(path_to_mapping_file, 'r') as file:
                    mapping = json.load(file)
                processed_data = Utils.rename_columns(processed_data, mapping)
            except Exception as e:
                raise Exception(f'Erro ao ler o arquivo mapping: {e}')

        # Gravando os resultados na camada staging        
        processed_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data.to_csv(processed_data_path, index=False, sep = separator)
    
    def load_stream(self, user, password, host, db_name, schema, mode = 'replace', **kwargs):
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        path_to_schema_file = kwargs.get('path_to_schema_file',None)
        loader = PostgresLoader(user=user, password=password, host=host, db_name=db_name)
        staged_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        staged_data = pd.read_csv(staged_data_path, sep=separator)
        loader.load_data(dataframe=staged_data, target_table=self.output_name, mode=mode, target_schema=schema, path_to_schema_file=path_to_schema_file)