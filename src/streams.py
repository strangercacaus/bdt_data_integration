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
        self.writer = DataWriter(source=self.source, stream=self.source_name, compression = False, config= self.config)
    
    def extract_stream(self, database_id, token) -> None:
        extractor = NotionDatabaseAPIExtractor(token = token, database_id = database_id)
        records = extractor.run()
        self.writer.dump_records(records, target_layer = 'raw', date=False)

    def transform_stream(self, entity: str = ['pages','users'], **kwargs) -> None:
        kwargs.get('')
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        transformer = NotionTransformer()
        source_name = kwargs.get('source_stream', self.source_name)

        path = f'/work/data/raw/{self.source}/{source_name}'
        extension = '.txt'
        if self.writer.compression == True:
                extension += '.gz'
        try:
            raw_data_path = Utils.get_latest_file(path, extension)
        except:
            raw_data_path = path + extension
        if raw_data_path == None:
            raise Exception(f'{__name__}: raw_data_path é vazio.')
        try:
            records = Utils.read_records(raw_data_path)
        except Exception as e:
            raise Exception(f'No files found in the specified directory: {raw_data_path} ({e})')
        
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
        self.writer.dump_csv(processed_data, output_path = processed_data_path, sep = separator)
        #processed_data.to_csv(processed_data_path, sep = separator, index=False, encoding='utf-8')
    
    def stage_stream(self, rename_columns:bool = False, **kwargs):
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        # Lendo o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data = pd.read_csv(processed_data_path, sep = separator, encoding='utf-8')

        if rename_columns:
            mapping_file_path = kwargs.get('mapping_file_path',None)
            if not mapping_file_path:
                raise Exception('Caminho do arquivo mapping não foi informado')
            try:
                with open(mapping_file_path, 'r') as file:
                    mapping = json.load(file)
                processed_data = Utils.rename_columns(processed_data, mapping)
            except Exception as e:
                raise Exception(f'Erro ao ler o arquivo mapping: {e}')

        staged_data_path = self.writer.get_output_file_path(output_name = self.output_name,target_layer='staging') + '.csv'
        os.makedirs(os.path.dirname(staged_data_path), exist_ok=True)
        self.writer.dump_csv(processed_data, output_path = staged_data_path, sep = separator)
        #processed_data.to_csv(staged_data_path, sep = separator, index=False, encoding='utf-8')
    
    def load_stream(self, user, password, host, db_name, schema, mode = 'replace', **kwargs):
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        schema_file_path = kwargs.get('schema_file_path',None)
        loader = PostgresLoader(user=user, password=password, host=host, db_name=db_name, schema_file_path = schema_file_path, schema_file_type = 'template')
        staged_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        staged_data = pd.read_csv(staged_data_path, sep=separator, encoding='utf-8')
        loader.load_data(df=staged_data, target_table=self.output_name, mode=mode, target_schema=schema)

class BenditoStream():
    def __init__(self, source_name,  config, **kwargs):
        self.source = 'bendito'
        self.config = config
        self.source_name = source_name 
        self.output_name = kwargs.get('output_name',self.source_name)
        self.writer = DataWriter(source=self.source, stream=self.source_name, compression = False, config= self.config)

    @property
    def schema(self):
        query = f"SELECT column_name, is_nullable, udt_name FROM information_schema.COLUMNS WHERE table_name = '{self.source_name}' ORDER BY table_name, ordinal_position"
        payload = json.dumps({"query": query, "separator":";"})
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            self.schema = pd.read_csv(StringIO(response.text),sep=";", encoding='utf-8')
        else:
            raise Exception(f'Exceção HTTP: {response.status_code}, {response.text}')
        
    def set_extractor(self, token, page_size = 500, separator = ';'):
        self.extractor = BenditoAPIExtractor(source = self.source, token = token, writer = self.writer, schema = self.schema)

    def extract_stream(self, custom_query=None, page_size=500, **kwargs) -> None:
        separator = kwargs.get('separator',';')
        
        if custom_query:
            query = custom_query.strip().rstrip(';')
        else:
            query = f"select * from {self.source_name}"

        records = self.extractor.run(
            query = query,
            separator = separator,
            page_size = page_size)
            
        raw_data_path = self.writer.get_output_file_path(target_layer='raw', date=False) + '.csv'
        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)
        self.writer.dump_csv(records, output_path = raw_data_path, sep = separator)
        #records.to_csv(raw_data_path, index=False, sep=separator, encoding='utf-8')
        return records
        
    def transform_stream(self, **kwargs) -> None:
        # Lendo o arquivo na camada raw
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        path = f'/work/data/raw/{self.source}/{self.source_name}'
        extension = '.csv'
        try:
            raw_data_path = Utils.get_latest_file(path, extension)
        except:
            raw_data_path = path + extension
        if raw_data_path == None:
            raise Exception(f'No files found in the specified directory: {file_path}')   
        records = pd.read_csv(raw_data_path,sep=separator, low_memory=False)

        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        self.writer.dump_csv(records, output_path = processed_data_path, sep = separator)
        #records.to_csv(processed_data_path, index=False, sep=separator, encoding='utf-8')
    
    def stage_stream(self, rename_columns=False, **kwargs):

        # Lendo o arquivo na camada processing

        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        processed_data_path = self.writer.get_output_file_path(target_layer='processing') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
        processed_data = pd.read_csv(processed_data_path, sep=separator, low_memory=False, encoding='utf-8')

        # Atualizando o nome das colunas conforme o mapping
        if rename_columns:
            mapping_file_path = kwargs.get('mapping_file_path',None)
            if not mapping_file_path:
                raise Exception('Caminho do arquivo mapping não foi informado')

            try:
                with open (mapping_file_path) as f:
                    mapping = json.load(f)
                    processed_data = Utils.rename_columns(processed_data, mapping)
            except Exception as e:
                raise Exception(f'Erro ao ler o arquivo mapping: {e}')

        # Gravando os resultados na camada staging        
        processed_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

        # Testando função própria para a escrita de csv
        self.writer.dump_csv(processed_data, output_path = processed_data_path, sep = separator)
        #processed_data.to_csv(processed_data_path, index=False, sep = separator, encoding='utf-8')
    
    def load_stream(self, user, password, host, db_name, schema, mode = 'replace', **kwargs):
        separator = kwargs.get('separator',self.config.get('DEFAULT_CSV_SEPARATOR',';'))
        schema_file_path = kwargs.get('schema_file_path',None)
        loader = PostgresLoader(user=user, password=password, host=host, db_name=db_name, schema_file_path = schema_file_path, schema_file_type = 'info_schema')
        staged_data_path = self.writer.get_output_file_path(output_name = self.output_name, target_layer='staging') + '.csv'
        staged_data = pd.read_csv(staged_data_path, sep=separator, low_memory = False, encoding='utf-8')
        loader.load_data(df=staged_data, target_table=self.output_name, mode=mode, target_schema=schema)