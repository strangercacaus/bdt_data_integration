import os
import glob
import yaml
import gzip
import json
import logging
import requests
import sqlparse
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Utils:
    """
    Classe utilitária que fornece métodos estáticos para operações comuns.

    Esta classe contém métodos para manipulação de arquivos, obtenção de datas formatadas, 
    busca de configurações e leitura de registros de arquivos. Todos os métodos são estáticos 
    e podem ser chamados sem a necessidade de instanciar a classe.
    """
    @staticmethod
    def validate_sql(query):
        try:
            parsed = sqlparse.parse(query)
            return bool(parsed)  # Returns True if there are parsed statements
        except Exception as e:
            return False

    @staticmethod
    def get_schema(schema): 
        token =  os.environ['BENDITO_BI_TOKEN']
        url = os.environ['BENDITO_BI_URL']
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}

        query = f"SELECT * FROM information_schema.COLUMNS WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position"
        payload = json.dumps({"query": query, "separator":";"})
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            return pd.read_csv(StringIO(response.text),sep=";")
        else:
            raise Exception(f'Exceção HTTP: {response.status_code}, {response.text}')

    @staticmethod
    def find_file(file_name):
        result = []
        for root, dirs, files in os.walk('/'):
            result.extend(os.path.join(root, file) for file in files if file == file_name)
        return result
        
    @staticmethod
    def get_latest_file(directory, extension):
        list_of_files = glob.glob(f'{directory}/*{extension}')
        path = max(list_of_files, key=os.path.getctime) if list_of_files else None
        if path is None:
            raise Exception(f'Arquivo não foi encotrado em {directory}')
        else:
            return path
    
    @staticmethod
    def rename_columns(df, mapping):
        """
        Renomeia as colunas de um df de acordo com o mapa de nomes fornecido.

        Args:
        df (pd.df): O df cujas colunas serão renomeadas.
        column_name_mapping (dict): Dicionário contendo o mapeamento de nomes de colunas antigos para novos.

        Returns:
        pd.df: df com as colunas renomeadas.
        """
        renamed_df = df.rename(columns=mapping)
        renamed_df_cols = renamed_df.columns
        new_cols = list(mapping.values())  # Use new names here
        return renamed_df[[col for col in new_cols if col in renamed_df_cols]]

    @staticmethod
    def get_current_formatted_date():
        """
        Obtém o arquivo mais recente em um diretório com a extensão especificada.

        Este método procura por arquivos no diretório fornecido que correspondem à extensão 
        especificada e retorna o arquivo mais recente com base na data de criação.

        Args:
            directory (str): O diretório onde procurar os arquivos.
            extension (str): A extensão dos arquivos a serem procurados.

        Returns:
            str: O caminho para o arquivo mais recente, ou None se nenhum arquivo for encontrado.
        """
        utc_offset = timedelta(hours=-3)
        return (datetime.now() + utc_offset).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def find_config_yaml():
        """
        Procura pelo arquivo 'config.yaml' no diretório 'config' dentro do diretório raiz.

        Este método percorre o diretório raiz em busca do arquivo de configuração e retorna 
        o caminho para o arquivo se encontrado.

        Returns:
            str: O caminho para o arquivo 'config.yaml' se encontrado, caso contrário, retorna None.
        """
        root_dir = os.getcwd()
        for dirpath, dirnames, filenames in os.walk(root_dir):
            if 'config' in dirnames:
                config_dir = os.path.join(dirpath, 'config')
                for file in os.listdir(config_dir):
                    if file == 'config.yaml':
                        return os.path.join(config_dir, file)
        return None

    @staticmethod
    def load_config():
        """
        Carrega a configuração do projeto do arquivo 'config.yaml'.

        Este método tenta localizar e carregar as configurações do arquivo 'config.yaml'. 
        Se o arquivo não for encontrado, retorna None.

        Returns:
            dict: A configuração obtida do arquivo 'config.yaml', ou None caso contrário.
        """
        if config_path := Utils.find_config_yaml():
            with open(config_path, 'r') as file:
                logger.info('Configuração YAML carregada.')
                return yaml.safe_load(file)
                

        else:
            raise ValueError('Arquivo de configuração da integração não foi encontrado')
    
    @staticmethod
    def read_records(file_path):
        """
        Lê registros de um arquivo JSON, suportando arquivos compactados.

        Este método lê registros de um arquivo especificado, que pode ser um arquivo JSON 
        normal ou um arquivo JSON compactado (.gz). Retorna uma lista de registros.

        Args:
            file_path (str): O caminho para o arquivo a ser lido.

        Returns:
            list: Uma lista de registros lidos do arquivo.
        """
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt') as f:
                records = [json.loads(line) for line in f.read().splitlines()]
        else:
            with open(file_path, 'r') as f:
                records = [json.loads(line) for line in f.read().splitlines()]
        return records
        