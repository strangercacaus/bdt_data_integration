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
    def __init__(self, source=None, stream=None, root=None) -> None:
        """
        Inicializa uma instância da classe DataWriter com parâmetros opcionais 
        para source, stream e root. Este construtor configura os atributos 
        necessários para que a instância funcione corretamente.

        Args:
            source (str, optional): O identificador da fonte dos dados.
            stream (str, optional): O identificador do fluxo dos dados.
            root (str, optional): O diretório raiz para operações de arquivo.

        Returns:
            None
        """
        self.source = source
        self.root = root
    
    @property
    def _get_config(self):
        """
        Carrega e recupera as configurações a partir de um arquivo YAML. Esta 
        propriedade fornece acesso aos dados de configuração necessários para 
        o funcionamento da classe.

        Returns:
            dict: As configurações carregadas do arquivo YAML.

        Raises:
            FileNotFoundError: Se o arquivo de configuração não existir.
            yaml.YAMLError: Se houver um erro ao analisar o arquivo YAML.
        """
        with open(f'{self.root}/config/{self.source}/config.yaml', 'r') as file:
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

    def _write_data(self, row:'str',output) ->None:
        """
        Escreve os dados fornecidos em um arquivo no caminho especificado. 
        Esta função é responsável por persistir os dados em um formato 
        adequado para uso posterior.

        Args:
            data: Os dados a serem escritos no arquivo.
            output_path (str): O caminho do arquivo onde os dados serão 
            armazenados.

        Raises:
            IOError: Se ocorrer um erro ao tentar escrever no arquivo.
        """           
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "a") as f:
            f.write(row)

    def dump_json_data(self, data: [List, dict], **kwargs): # type: ignore
        """
        Serializa e grava dados em formato JSON em um arquivo, com base no 
        diretório de destino especificado. Esta função aceita tanto dicionários 
        quanto listas e determina o caminho do arquivo de saída com base no 
        parâmetro de destino.

        Args:
            data (List or dict): Os dados a serem serializados e gravados. 
            Pode ser um dicionário ou uma lista de elementos.
            **kwargs: Parâmetros adicionais que podem incluir 'source', 
            'stream' e 'target'.

        Raises:
            DataTypeNotSupportedForIngestionException: Se os dados fornecidos 
            não forem um dicionário nem uma lista.
        """ 
        source = kwargs.get("source")
        stream = kwargs.get("stream")
        target = kwargs.get("target")
        date = utils.get_current_formatted_date()

        if target == 'raw':
            output = f'{self._get_raw_dir()}/{source}/{stream}/{date}.txt'
        elif target == 'processing':
            output = f'{self._get_processing_dir()}/{source}/{date}.txt'
        elif target == 'staging':
            output = f'{self._get_staging_dir()}/{source}.txt'
        else:
            print('Target must be one of "raw", "processing" or "staging"')

        if isinstance(data, dict):
            row = json.dumps(data) + "\n"
            self._write_row(row,output)
        elif isinstance(data, List):
            for row in data:
                self._write_row(row,output)
        else:
            raise DataTypeNotSupportedForIngestionException(data)