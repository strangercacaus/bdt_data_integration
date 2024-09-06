import os
import json
import gzip
import yaml
from typing import List
import logging

from abc import ABC, abstractmethod
from work.bdt_data_integration.src.utils import Utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataTypeNotSupportedException(Exception):
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
    def __init__(self, source=None, stream=None, root=None, config=None, compression:bool=False) -> None:
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
        self.stream = stream 
        self.config = config
        self.compression = compression

    def _get_raw_dir(self):
        """
        Retorna o diretório onde os dados brutos são armazenados. Esta 
        propriedade fornece o caminho necessário para acessar os dados 
        não processados.

        Returns:
            str: O caminho do diretório de dados brutos.
        """
        return self.config["RAW_DIR"]

    def _get_processing_dir(self):
        """
        Retorna o diretório onde os dados em processamento são armazenados. 
        Esta propriedade fornece o caminho necessário para acessar os dados 
        que estão sendo processados.

        Returns:
            str: O caminho do diretório de dados em processamento.
        """
        return self.config["PROCESSING_DIR"]

    def _get_staging_dir(self):
        """
        Retorna o diretório onde os dados em estágio são armazenados. Esta 
        propriedade fornece o caminho necessário para acessar os dados que 
        estão prontos para serem processados ou transferidos.

        Returns:
            str: O caminho do diretório de dados em estágio.
        """
        return self.config["STAGING_DIR"]
        
    def _write_row(self, rows:'List[str]', filename:str = None, file_format=None) -> None:
        """
        Escreve os dados fornecidos em um arquivo no caminho especificado, utilizando
        uma lista de strings para melhorar a performance ao escrever múltiplas linhas.

        Args:
            rows (List[str]): As linhas de dados a serem escritas no arquivo.
            output (str): O caminho do arquivo onde os dados serão armazenados.
            compression (bool): Indica se a compressão gzip deve ser usada.

        Raises:
            IOError: Se ocorrer um erro ao tentar escrever no arquivo.
        """
        compression = self.compression
        if filename == None:
            raise ValueError ('Invalid filename on _write_row')
        filename = filename + file_format + '.gz' if compression else filename + file_format
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        if not compression:
            with open(filename, "wt", encoding='utf-8') as file:
                file.writelines(rows)
        else:
            with gzip.open(filename, 'wt', encoding='utf-8') as file:
                file.writelines(rows)


    def get_output_file_path(
                            self,
                            source: str = 'default',
                            stream: str = 'default',
                            filename: str = '',
                            page_number: int = None,
                            page_suffix: str = 'Page',
                            target_layer: str = None,
                            date: bool = False,
                            ):

        current_date = Utils.get_current_formatted_date() if date else None
        
        if target_layer == 'raw':
            target_dir = self._get_raw_dir()
        elif target_layer == 'processing':
            target_dir = self._get_processing_dir()
        elif target_layer == 'staging':
            target_dir = self.get_staging_dir()

        path = f'{target_dir}/{source}/{stream}'

        if date:
            path += f'/{current_date}'

        if page_number:
            path += f'/{page_suffix}{page_number}'
        
        if filename:
            path += '/{filename}'

        return path


    def dump_records(self,
                     records: [list, dict],
                     target_layer: str = None,
                     page_number: int = None,
                     file_format: str = '.txt',
                     date:bool=False) -> None:
        """
        Serializa e escreve dados JSON em um arquivo com base no diretório de destino especificado.
        Esta função aceita tanto dicionários quanto listas e determina o caminho
        do arquivo de saída com base no parâmetro de destino.

        Args:
            records (list ou dict): Dados a serem serializados e escritos. Pode ser um dicionário ou uma lista de elementos.
            target_layer (str): Diretório de destino ("raw", "processing", "staging").
            page_number (int, opcional): Número da página a ser incluído no caminho do arquivo de saída. Default é None.
            file_format (str): A extensão do arquivo de saída. Default é '.txt'.

        Raises:
            ValueError: Se o "target_layer" não for nenhum de "raw", "processing" ou "staging".
            DataTypeNotSupportedForIngestionException: Se os dados fornecidos não forem nem um dicionário nem uma lista.
        """
        if target_layer is None:
            raise ValueError('"target_layer" must be one of "raw", "processing" or "staging"')

        output = self.get_output_file_path(source=self.source, stream=self.stream, target_layer=target_layer, page_number=page_number, date=date)

        if isinstance(records, dict):
            rows = [json.dumps(records) + "\n"]
        elif isinstance(records, list):
            rows = [json.dumps(record) + "\n" for record in records]
        else:
            raise DataTypeNotSupportedException(records)

        self._write_row(rows, output, file_format)