import json
import os
import yaml
from typing import List
from abc import ABC, abstractmethod
from src.utils import utils

class DataTypeNotSupportedForIngestionException(Exception):
    """
    Exceção levantada quando um tipo de dado não é adequado para 
    a escrita através do método especificado.
        Esta exceção é utilizada para indicar que o tipo de 
    dado fornecido não é suportado pelo processo de ingestão.

    Args:
        data_type (str): O tipo de dado que não é suportado.
        message (str, optional): Mensagem de erro personalizada.
    """

    def __init__(self, data_type, message="Tipo de dado não suportado para ingestão."):
        self.data_type = data_type
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} (Tipo de dado: {self.data_type})"


class DataWriter(ABC):
    """
    A classe datawriter centraliza a responsabilidade da escrita de dados no repósitório.
    Também e a responsável por reforçar as regras de negócio de uso das camadas raw, processing e staging,
    através de funções especificas para a manipulação de objetos nestas camadas.
    """
    def __init__(self, resource=None) -> None:
        self.source = source
    
    @property
    def _get_config():
        with open(f'/work/config/{source}/config.yaml', 'r') as file:
            return yaml.safe_load(file)

    @property
    def _get_raw_dir(self):
        """
        Retorna o diretório onde os dados brutos são armazenados. Esta 
        propriedade fornece o caminho necessário para acessar os dados 
        não processados.

        Returns:
            str: O caminho do diretório de dados brutos.
        """
        return self.config["RAW_DIR"]

    @property
    def _get_processing_dir(self):
        """
        Retorna o diretório onde os dados em processamento são armazenados. 
        Esta propriedade fornece o caminho necessário para acessar os dados 
        que estão sendo processados.

        Returns:
            str: O caminho do diretório de dados em processamento.
        """
        return self.config["PROCESSING_DIR"]

    @property
    def _get_staging_dir(self):
        """
        Retorna o diretório onde os dados em estágio são armazenados. Esta 
        propriedade fornece o caminho necessário para acessar os dados que 
        estão prontos para serem processados ou transferidos.

        Returns:
            str: O caminho do diretório de dados em estágio.
        """
        return self.config["STAGING_DIR"]

    @abstractmethod
    def _write_row(self, row:'str') ->None:
        # os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        # with open(self.filename, "a") as f:
        #     f.write(row)
        pass

    @abstractmethod
    def write(self, data: [List, dict]):
        # if isinstance(data, dict):
        #     self._write_row(json.dumps(data) + "\n")
        # elif isinstance(data, List):
        #     for element in data:
        #         self.write(element)
        # else:
        #     raise DataTypeNotSupportedForIngestionException(data)
        pass