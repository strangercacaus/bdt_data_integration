# Importação de módulos.
import os
import sys
import json
import pandas as pd

# Adicionando diretório dos módulos personalizados ao PATH
sys.path.append(os.path.abspath('bdt_data_integration'))

# Importando módulos personalizadso
from src.writers import DataWriter
from src.loaders import PostgresLoader
from src.transformers import NotionTransformer
from src.extractors import NotionDatabaseAPIExtractor
from src.utils import Utils, WebhookNotifier, DiscordNotifier

class NotionStream():
    def __init__(self, stream_name, config):
        self.stream_name = stream_name
        self.source = 'notion'
        self.prefix = 'ntn__'
        self.output_table = self.prefix + self.stream_name
        self.config = config
        self.writer = DataWriter(source=self.source, stream=self.stream_name, compression = True, config= self.config)
    
    def extract_database(self, database_id, token) -> None:
        extractor = NotionDatabaseAPIExtractor(token = token, database_id = database_id)
        records = extractor.run()
        self.writer.dump_records(records, target_layer = 'raw', date=True)
    
    def extract_stream(self, database_id, token) -> None:
        extractor = NotionDatabaseAPIExtractor(token = token, database_id = database_id)
        records = extractor.run()
        self.writer.dump_records(records, target_layer = 'raw', date=True)

    def transform_stream(self, entity: str = ['pages','users'], **kwargs) -> None:
        transformer = NotionTransformer()
        stream_name = kwargs.get('source_stream',self.stream_name)
        file_path = Utils.get_latest_file(f'/work/data/raw/{self.source}/{stream_name}', '.txt.gz')
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
            mapping_file_path = f'/work/schema/{self.output_table}.json'
            with open(mapping_file_path, 'r') as file:
                mapping = json.load(file)
        except:
            mapping_file_path = f'/datasets/_deepnote_work/schema/{self.output_table}.json'
            with open(mapping_file_path, 'r') as file:
                mapping = json.load(file)
        processed_data = Utils.rename_columns(processed_data, mapping)
        output = self.writer.get_output_file_path(output_table = self.output_table,target_layer='staging') + '.csv'
        os.makedirs(os.path.dirname(output), exist_ok=True)
        processed_data.to_csv(output, index=False)
    
    def load_stream(self, user, password, host, db_name, schema, mode = 'replace'):
        loader = PostgresLoader(user=user, password=password, host=host, db_name=db_name)
        
        staged_data_path = self.writer.get_output_file_path(output_table = self.output_table, target_layer='staging') + '.csv'
        staged_data = pd.read_csv(staged_data_path)
        loader.load_data(dataframe=staged_data, target_table=self.output_table, mode=mode, target_schema=schema)