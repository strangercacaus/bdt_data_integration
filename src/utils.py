import os
from datetime import datetime, timedelta
import yaml

class Utils:
    @staticmethod
    def get_current_formatted_date():
        """
        Get the current date formatted with a UTC offset of -3 hours.

        Returns:
            date: The current date with the specified UTC offset.
        """
        utc_offset = timedelta(hours=-3)
        return (datetime.now() + utc_offset).date()

    @staticmethod
    def find_config_yaml():
        """
        Search for the 'config.yaml' file in the 'config' directory within the root directory.

        Returns:
            str: The path to the 'config.yaml' file if found, otherwise None.
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
            dict: A configuração obtida do arquivo config.yaml, falso caso contrário.
        """
        config_path = Utils.find_config_yaml()
        if config_path:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        return None