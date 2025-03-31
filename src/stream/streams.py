# Importação de módulos.

import os
import sys
import json
import pandas as pd
import logging
import requests
from io import StringIO

# Adicionando diretório dos módulos personalizados ao PATH
sys.path.append(os.path.abspath('bdt_data_integration'))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Importando módulos personalizadso
from writer.writers import DataWriter
from loader.loaders import PostgresLoader
from transformer.transformers import NotionTransformer
from src.extractor import NotionDatabaseAPIExtractor, BenditoAPIExtractor, BitrixAPIExtractor
from src.util import Utils, WebhookNotifier, Schema

class NotionStream():
    
    def __init__(self, source_name, config, **kwargs):
        self.source = 'notion'
        self.config = config
        self.source_name = source_name
        self.output_name = kwargs.get(
            'output_name',
            self.source_name
            )
        self.writer = DataWriter(
            source = self.source,
            stream=self.source_name,
            compression = False,
            config= self.config
            )
    
    def set_extractor(self, database_id, token):

        self.extractor = NotionDatabaseAPIExtractor(
            token = token,
            database_id = database_id
            )
    
    def extract_stream(self) -> None:

        records = self.extractor.run()

        self.writer.dump_records(
            records,
            target_layer = 'raw',
            date = False
            )

    def transform_stream(self, entity: str = None, **kwargs) -> None: 

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        transformer = NotionTransformer()

        source_name = kwargs.get(
            'source_stream',
            self.source_name
            )

        path = f'/work/data/raw/{self.source}/{source_name}'

        extension = '.txt'
        
        if self.writer.compression == True:
                extension += '.gz'
    
        try:
            raw_data_path = Utils.get_latest_file(
                path,
                extension
                )

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
                processed_data['Duração da Tarefa']  = processed_data['Duração da Tarefa'].apply(lambda x: x['start'] if isinstance(x, dict) and 'start' in x else None)

        elif entity == 'users':
            processed_data = transformer._extract_users_list(records)

        # Gravando o arquivo na camada processing
        processed_data_path = self.writer.get_output_file_path(
            target_layer = 'processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        #self.writer.dump_csv(processed_data, output_path = processed_data_path, sep = separator)

        processed_data.to_csv(
            processed_data_path,
            sep = separator,
            index = False,
            encoding='utf-8'
            )
    
    def stage_stream(self, rename_columns:bool = False, **kwargs):

        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR',
            ';'
            )
        )

        # Lendo o arquivo na camada processing

        processed_data_path = self.writer.get_output_file_path(
            target_layer = 'processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        processed_data = pd.read_csv(
            processed_data_path,
            sep = separator,
            encoding='utf-8',
            dtype = str
            )

        if rename_columns:
            mapping_file_path = kwargs.get('mapping_file_path',None)
            if not mapping_file_path:
                raise Exception('Caminho do arquivo mapping não foi informado')
            try:
                with open(mapping_file_path, 'r') as file:
                    mapping = json.load(file)

                processed_data = Utils.rename_columns(
                    processed_data,
                    mapping
                    )

            except Exception as e:
                raise Exception(f'Erro ao ler o arquivo mapping: {e}') from e

        else:
            processed_data.columns = processed_data.columns.str.lower()

        staged_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer ='staging'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(staged_data_path),
            exist_ok = True
            )

        #self.writer.dump_csv(processed_data, output_path = staged_data_path, sep = separator)

        processed_data.to_csv(
            staged_data_path,
            sep = separator,
            index = False,
            encoding='utf-8'
            )
    
    def set_loader(self, user, password, host, db_name, schema_file_path, schema_file_type):

        self.loader = PostgresLoader(
            user = user,
            password = password,
            host = host,
            db_name = db_name,
            schema_file_path = schema_file_path,
            schema_file_type = 'template'
            )
    
    def load_stream(self, target_schema, **kwargs):

        mode = kwargs.get(
            'mode',
            'replace'
            )

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        schema_file_path = kwargs.get(
            'schema_file_path',
            None
            )

        staged_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer = 'staging'
            ) + '.csv'

        staged_data = pd.read_csv(
            staged_data_path,
            sep=separator,
            encoding='utf-8'
            )

        self.loader.load_data(
            df = staged_data,
            target_table = self.output_name,
            mode = mode, 
            target_schema = target_schema
            )

class BenditoStream():

    def __init__(self, source_name,  config, **kwargs):
        self.source = 'bendito'
        self.config = config
        self.source_name = source_name 
        self.output_name = kwargs.get(
            'output_name',
            self.source_name
            )
        self.writer = DataWriter(
            source=self.source,
            stream=self.source_name,
            compression = False,
            config= self.config
            )

    def set_extractor(self, token, separator = ';'):
        self.extractor = BenditoAPIExtractor(
            source = self.source,
            token = token,
            writer = self.writer,
            separator = separator
            )

    def extract_stream(self, custom_query=None, page_size=500, **kwargs) -> None:

        separator = kwargs.get(
            'separator',
            ';'
            )

        order_col = kwargs.get(
            'order_col',
            1
            )
        
        if custom_query:
            query = custom_query.strip().rstrip(';')
        else:
            query = f'select * from "{self.source_name}" order by {order_col} asc'

        records = self.extractor.run(
            query = query,
            separator = separator,
            page_size = page_size)
            
        raw_data_path = self.writer.get_output_file_path(
            target_layer = 'raw',
            date = False
            ) + '.csv'

        os.makedirs(
            os.path.dirname(raw_data_path),
            exist_ok = True
            )

        #self.writer.dump_csv(records, output_path = raw_data_path, sep = separator)

        records.to_csv(
            raw_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )

        return records
        
    def transform_stream(self, **kwargs) -> None:

        # Lendo o arquivo na camada raw
        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        path = f'/work/data/raw/{self.source}/{self.source_name}'

        extension = '.csv'

        try:
            raw_data_path = Utils.get_latest_file(path, extension)
        except Exception:
            raw_data_path = path + extension
        if raw_data_path is None:
            raise Exception(f'No files found in the specified directory: {file_path}')

        records = pd.read_csv(
            raw_data_path,
            sep = separator,
            dtype = str
            )

        records.replace(
            {
                'System.String[]': None,    # Substituir tipo .NET string array type com None.
                'System.Int32[]': None,     # Substituir tipo .NET integer array type com None.
                'System.Double[]': None,    # Substituir tipo .NET double array type com None.
                'System.Boolean[]': None    # Substituir tipo .NET boolean array type com None.
            },
            inplace=True
            )

        processed_data_path = self.writer.get_output_file_path(
            target_layer = 'processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        #self.writer.dump_csv(records, output_path = processed_data_path, sep = separator)

        records.to_csv(
            processed_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )
    
    def stage_stream(self, rename_columns=False, **kwargs):

        # Lendo o arquivo na camada processing

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        processed_data_path = self.writer.get_output_file_path(
            target_layer = 'processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok=True
            )

        processed_data = pd.read_csv(
            processed_data_path,
            sep = separator,
            encoding='utf-8',
            dtype=str
            )

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
                raise Exception(f'Erro ao ler o arquivo mapping: {e}') from e

        # Gravando os resultados na camada staging        
        processed_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer='staging'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        # Testando função própria para a escrita de csv
        #self.writer.dump_csv(processed_data, output_path = processed_data_path, sep = separator)
        processed_data.to_csv(
            processed_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )

    def set_loader(self, user, password, host, db_name, schema_file_path, schema_file_type):

        self.loader = PostgresLoader(
            user = user,
            password = password,
            host = host,
            db_name = db_name,
            schema_file_path = schema_file_path,
            schema_file_type = schema_file_type
            )
    
    def load_stream(self,target_schema, **kwargs):

        mode = kwargs.get(
            'mode',
            'replace'
            )

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        schema_file_path = kwargs.get(
            'schema_file_path',
            None
            )

        staged_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer='staging'
            ) + '.csv'

        staged_data = pd.read_csv(
            staged_data_path,
            sep = separator,
            encoding='utf-8',
            dtype=str
            )

        self.loader.load_data(
            df = staged_data,
            target_table = self.output_name,
            mode = mode,
            target_schema = target_schema
            )


class BitrixStream():
    def __init__(self, source_name,  config, **kwargs):
        self.source = 'bitrix'
        self.config = config
        self.source_name = source_name 
        self.output_name = kwargs.get(
            'output_name',
            self.source_name
            )
    
    @property
    def writer(self):
        return DataWriter(
            source=self.source,
            stream=self.source_name,
            compression = False,
            config= self.config
            )

    def set_extractor(self,**kwargs):

        separator = kwargs.get(
            'separator',
            ';'
            )

        token = kwargs.get(
            'token',
            None
            )

        bitrix_url = kwargs.get(
            'bitrix_url',
            None
            )

        bitrix_user_id = kwargs.get(
            'bitrix_user_id'
            ,None
            )

        if any(var is None for var in [token, bitrix_url, bitrix_user_id]):
            raise ValueError('Variável obrigatória omitida em BitrixStream.set_extractor')

        self.extractor = BitrixAPIExtractor(
            source = self.source,
            token = token,
            writer = self.writer,
            separator = separator,
            bitrix_url = bitrix_url,
            bitrix_user_id = bitrix_user_id
            )

    def set_schema(self):
        self.schema = self.extractor.extract_schema(self.source_name)

    def extract_stream(self, **kwargs) -> None:

        separator = kwargs.get(
            'separator',
            ';'
            )

        start = kwargs.get(
            'start',
            0
            )

        records = self.extractor.run(
            self.source_name
            )
            
        raw_data_path = self.writer.get_output_file_path(
            target_layer='raw',
            date=False
            ) + '.csv'

        os.makedirs(
            os.path.dirname(raw_data_path),
            exist_ok = True
            )

        records.to_csv(
            raw_data_path,
            index=False, sep = separator,
            encoding='utf-8'
            )

        return records
        
    def transform_stream(self, **kwargs) -> None:
        # Lendo o arquivo na camada raw

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        path = f'/work/data/raw/{self.source}/{self.source_name}'

        extension = '.csv'
        
        try:
            raw_data_path = Utils.get_latest_file(
                path,
                extension
                )

        except Exception:
            raw_data_path = path + extension
            
        if raw_data_path is None:
            raise Exception(f'No files found in the specified directory: {file_path}')
            
        records = pd.read_csv(
            raw_data_path,
            sep=separator,
            dtype=str
            )

        #records.columns = records.columns.str.lower()

        processed_data_path = self.writer.get_output_file_path(
            target_layer = 'processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        records.to_csv(
            processed_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )
    
    def stage_stream(self, rename_columns=False, **kwargs):

        # Lendo o arquivo na camada processing

        separator = kwargs.get(
            'separator',
            self.config.get(
                'DEFAULT_CSV_SEPARATOR',
                ';'
                )
            )

        processed_data_path = self.writer.get_output_file_path(
            target_layer='processing'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok=True
            )

        processed_data = pd.read_csv(
            processed_data_path,
            sep=separator,
            encoding='utf-8',
            dtype=str
            )

        # Atualizando o nome das colunas conforme o mapping
        # if rename_columns:
        #     mapping_file_path = kwargs.get('mapping_file_path',None)
        #     if not mapping_file_path:
        #         raise Exception('Caminho do arquivo mapping não foi informado')

        #     try:
        #         with open (mapping_file_path) as f:
        #             mapping = json.load(f)
        #             processed_data = Utils.rename_columns(processed_data, mapping)
        #     except Exception as e:
        #         raise Exception(f'Erro ao ler o arquivo mapping: {e}') from e

        # Gravando os resultados na camada staging 
               
        processed_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer='staging'
            ) + '.csv'

        os.makedirs(
            os.path.dirname(processed_data_path),
            exist_ok = True
            )

        processed_data.to_csv(
            processed_data_path,
            index = False,
            sep = separator,
            encoding = 'utf-8'
            )

    def set_loader(self, user, password, host, db_name, schema_file_type):

        self.loader = PostgresLoader(
            user = user,
            password = password,
            host = host,
            db_name = db_name,
            schema_file_type = schema_file_type
            )
    
    def load_stream(self, target_schema, target_table, **kwargs):

        mode = kwargs.get(
            'mode',
            'replace'
            )

        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR',';')
            )

        staged_data_path = self.writer.get_output_file_path(
            output_name = self.output_name,
            target_layer='staging'
            ) + '.csv'

        staged_data = pd.read_csv(
            staged_data_path,
            sep=separator,
            encoding='utf-8',
            dtype=str)

        self.loader.load_data(
            df = staged_data,
            target_table = target_table,
            target_schema = target_schema,
            mode = mode,
            schema = self.schema.render_ddl(target_schema,target_table))
