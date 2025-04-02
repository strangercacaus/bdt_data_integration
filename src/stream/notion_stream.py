import os
import json
import pandas as pd
import logging

# Adicionando diretório dos módulos personalizados ao PATH

from .base_stream import Stream
from writers import DataWriter
from loader.postgres_loader import PostgresLoader
from transformers import NotionTransformer
from extractor.notion_extractor import NotionDatabaseAPIExtractor
from utils import Utils
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)

class NotionStream(Stream):
    
    def __init__(self, source_name, config, **kwargs):
        """
        Inicializa uma NotionStream com um nome de fonte e uma configuração.
        
        Args:
            source_name (str): Nome da fonte da stream
            config (dict): Dicionário de configuração
            **kwargs: Argumentos adicionais
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
        Configura o NotionDatabaseAPIExtractor para esta stream.
        
        Args:
            database_id (str): ID do banco de dados Notion
            token (str): Token da API Notion
        """
        self.extractor = NotionDatabaseAPIExtractor(
            token=token,
            database_id=database_id
        )
    
    def extract_stream(self) -> None:
        """
        Extrai dados da API Notion e escreve para a camada raw.
        """
        records = self.extractor.run()

        self.writer.dump_records(
            records,
            target_layer='raw',
            date=False
        )

    def transform_stream(self, entity: str = None, **kwargs) -> None:
        """
        Transforma os dados brutos e escreve para a camada processing.
        
        Args:
            entity (str, optional): Tipo de entidade a ser transformada ('pages' ou 'users')
            **kwargs: Argumentos adicionais para a transformação
        """
        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR', ';')
        )

        transformer = NotionTransformer()

        source_name = kwargs.get('source_stream', self.source_name)

        # Usar caminho relativo em vez de absoluto
        base_dir = os.path.join(os.getcwd(), 'data')
        path = os.path.join(base_dir, 'raw', self.source, source_name)
        
        # Criar diretórios se não existirem
        os.makedirs(os.path.dirname(path), exist_ok=True)

        extension = '.txt'
        
        if self.writer.compression == True:
            extension += '.gz'
    
        try:
            raw_data_path = Utils.get_latest_file(path, extension)
        except Exception:
            raw_data_path = path + extension

        if raw_data_path is None:
            raise Exception(f'{__name__}: raw_data_path é vazio.')

        # Verificar se o arquivo existe antes de tentar lê-lo
        if not os.path.exists(raw_data_path):
            raise Exception(
                f'Arquivo não encontrado: {raw_data_path}. Execute extract_stream() antes de transform_stream().'
            )

        try:
            records = Utils.read_records(raw_data_path)
        except Exception as e:
            raise Exception(
                f'Nenhum arquivo encontrado no diretório especificado: {raw_data_path} ({e})'
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
        Processa os dados transformados e escreve para a camada staging.
        
        Args:
            rename_columns (bool): Se deve renomear colunas usando um arquivo de mapeamento
            **kwargs: Argumentos adicionais para a etapa de staging
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
    
    def set_loader(self, engine, schema_file_path, schema_file_type):
        """
        Configura o PostgresLoader para esta stream.
        
        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine para a conexão com o banco de dados
            schema_file_path (str): Caminho para o arquivo de esquema para criar tabelas
            schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema
        """
        self.loader = PostgresLoader(engine, schema_file_path, schema_file_type)
    
    def load_stream(self, target_schema, **kwargs):
        """
        Carrega os dados na camada staging no banco de dados de destino.
        
        Args:
            target_schema (str): Nome do esquema de destino
            **kwargs: Argumentos adicionais para o carregamento
        """
        mode = kwargs.get('mode', 'replace')
        
        separator = kwargs.get(
            'separator',
            self.config.get('DEFAULT_CSV_SEPARATOR', ';')
        )

        staged_data_path = self.writer.get_output_file_path(
            output_name=self.output_name,
            target_layer='staging'
        ) + '.csv'

        staged_data = pd.read_csv(
            staged_data_path,
            sep=separator,
            encoding='utf-8'
        )
        logger.info(f'Chamando load_data com staged_data.shape: {staged_data.shape}')
        self.loader.load_data(
            df=staged_data,
            target_schema=target_schema,
            target_table=self.output_name,
            mode=mode
        ) 