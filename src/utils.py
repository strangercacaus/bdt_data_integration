import os
import glob
import yaml
import gzip
import json
import discord
import requests
from datetime import datetime, timedelta

class Utils:
    
    @staticmethod
    def get_latest_file(directory, extension):
        list_of_files = glob.glob(f'{directory}/*{extension}')
        if not list_of_files:
            return None
        latest_file = max(list_of_files, key=os.path.getctime)
        return latest_file

    @staticmethod
    def get_current_formatted_date():
        """
        Obtém a data atual formatada com um deslocamento UTC de -3 horas.

        Returns:
            date: A data atual no formato 'YYYY-MM-DD HH:MI:SS' com o deslocamento UTC especificado.
        """
        utc_offset = timedelta(hours=-3)
        return (datetime.now() + utc_offset).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def find_config_yaml():
        """
        Procura pelo arquivo 'config.yaml' no diretório 'config' dentro do diretório raiz.

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
        Carrega a configuração do projeto do arquivo config.yaml

        Retorna:
            dict: A configuração obtida do arquivo config.yaml, None caso contrário.
        """
        if config_path := Utils.find_config_yaml():
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        else:
            return None
    
    @staticmethod
    def read_records(file_path):
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt') as f:
                records = [json.loads(line) for line in f.read().splitlines()]
        else:
            with open(file_path, 'r') as f:
                records = [json.loads(line) for line in f.read().splitlines()]
        return records

class WebhookNotifier:

    def __init__(self, url, pipeline):
        self.url = url
        self.pipeline = pipeline

    def pipeline_start(self):
        url = self.url
        payload = json.dumps({"message": f'Iniciando pipeline: {self.pipeline}'})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)

    def pipeline_end(self):
        url = self.url
        payload = json.dumps({"message": f'Execução de pipeline encerrada: {self.pipeline}'})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)

    def pipeline_error(self, e=None):
        url = self.url
        payload = json.dumps({"message": f'Erro na execução do pipeline: {self.pipeline}.\n{e}'})
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)

    def error_handler(self, func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.pipeline_error(e)
                raise e # Re-raise the exception after logging it
        return wrapper

class DiscordNotifier(discord.Client):

    def __init__(self, token, channel_id, pipeline):
        self.token = token
        self.channel_id = channel_id
        self.pipeline = pipeline
        self.client = discord.client
