import os
import glob
import yaml
import gzip
import json
import logging
import discord
import requests
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
                return yaml.safe_load(file)
        else:
            return None
        logger.info('Configuração YAML carregada.')
    
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

class WebhookNotifier:
    """
    Classe para notificação de eventos de pipeline via webhook.

    Esta classe permite enviar notificações sobre o início, fim e erros de execução de um pipeline 
    para uma URL especificada. As mensagens são enviadas no formato JSON.

    Atributos:
        url (str): A URL do webhook para onde as notificações serão enviadas.
        pipeline (str): O nome do pipeline que está sendo monitorado.
    """
    def __init__(self, url, pipeline):
        """
        Inicializa o WebhookNotifier com a URL do webhook e o nome do pipeline.

        Args:
            url (str): A URL do webhook para onde as notificações serão enviadas.
            pipeline (str): O nome do pipeline que está sendo monitorado.
        """
        self.url = url
        self.pipeline = pipeline

    def pipeline_start(self, **kwargs):
        """
        Envia uma notificação de início de execução do pipeline.

        Este método envia uma mensagem para a URL do webhook informando que o pipeline 
        especificado foi iniciado.
        
        Returns:
            None
        """
        text = kwargs.get('text',None)
        message = text if text else f'Iniciando pipeline: {self.pipeline}'
        url = self.url
        payload = json.dumps({"message": f'Iniciando pipeline: {self.pipeline}'})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        logger.info(f"Make.Com Response: {response.text}")

    def pipeline_end(self, **kwargs):
        """
        Envia uma notificação de fim de execução do pipeline.

        Este método envia uma mensagem para a URL do webhook informando que a execução 
        do pipeline especificado foi encerrada.

        Returns:
            None
        """
        text = kwargs.get('text',None)
        message = text if text else f'Execução de pipeline encerrada: {self.pipeline}'
        url = self.url
        payload = json.dumps({"message": message})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        logger.info(f"Make.Com Response: {response.text}")

    def pipeline_error(self, e=None):
        """
        Envia uma notificação de erro na execução do pipeline.
        
        Args:
            e (Exception, optional): A exceção que ocorreu durante a execução do pipeline.
            
        Returns:
            None
        """
        url = self.url
        # Convert the exception to a string and split it into lines if needed
        error_message = str(e).splitlines()[:2]  # Take only the first 2 lines of the error message
        payload = json.dumps({"message": f'Erro na execução do pipeline: {self.pipeline}.\n{" ".join(error_message)}'})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        logger.info(f"Make.Com Response: {response.text}")

    def error_handler(self, func):
        """
        Decorador para tratar erros durante a execução de funções.

        Este método envolve uma função com um manipulador de erros que, em caso de exceção, 
        envia uma notificação de erro para o webhook e re-lança a exceção. Isso permite que 
        erros sejam registrados e tratados de forma centralizada.

        Args:
            func (callable): A função a ser decorada, que será monitorada para erros.

        Returns:
            callable: A função decorada com tratamento de erros, que envia notificações em caso de falha.

        Raises:
            Exception: Re-lança a exceção original após o registro do erro.
        """
        def wrapper(*args, **kwargs):
            """
            Função interna que envolve a execução de uma função decorada.

            Esta função tenta executar a função original com os argumentos fornecidos. 
            Se ocorrer uma exceção, ela registra o erro usando o método de notificação de erro 
            e re-lança a exceção para que possa ser tratada em outro lugar.

            Args:
                *args: Argumentos posicionais a serem passados para a função decorada.
                **kwargs: Argumentos nomeados a serem passados para a função decorada.

            Returns:
                qualquer: O resultado da função decorada, se executada com sucesso.

            Raises:
                Exception: Re-lança a exceção original após o registro do erro.
            """
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.pipeline_error(e)
                # raise e # Re-raise the exception after logging it
        return wrapper

class DiscordNotifier(discord.Client): # Implementar o notificador via discord para diminuir o consumo da API do Notion e permitir a personalização do Bot.

    def __init__(self, token, channel_id, pipeline):
        self.token = token
        self.channel_id = channel_id
        self.pipeline = pipeline
        self.client = discord.client
        