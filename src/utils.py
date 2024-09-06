import os
from datetime import datetime, timedelta
import yaml

class Utils:
    @staticmethod
    def get_current_formatted_date():
        """
        Obtém a data atual formatada com um deslocamento UTC de -3 horas.

        Returns:
            date: A data atual no formato 'YYYY-MM-DD' com o deslocamento UTC especificado.
        """
        utc_offset = timedelta(hours=-3)
        return (datetime.now() + utc_offset).date()

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