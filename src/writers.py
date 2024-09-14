import os
import json
import gzip
import logging
from typing import List
from src.utils import Utils
from abc import ABC, abstractmethod

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
    def __init__(self, config: dict, source:str = 'default', stream:str = 'default', compression: bool = False) -> None:
        """
        Inicializa uma instância da classe DataWriter com parâmetros opcionais 
        para source, stream e config. Este construtor configura os atributos 
        necessários para que a instância funcione corretamente, incluindo a opção 
        de compressão dos dados.

        Args:
            config (dict, obrigatório): Um dicionário contendo as configurações necessárias para a operação do DataWriter.
            source (str, optional): O identificador da fonte dos dados. O padrão é 'default'.
            stream (str, optional): O identificador do fluxo dos dados. O padrão é 'default'.
            compression (bool, optional): Indica se a compressão dos dados deve ser aplicada. O padrão é False.
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
        Retorna o diretório onde os dados em preparação para o carregamento são armazenados. Esta 
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
            filename (str, optional): O caminho do arquivo onde os dados serão armazenados. Deve ser fornecido.
            file_format (str, optional): A extensão do arquivo a ser utilizada. Se a compressão estiver ativada, '.gz' será adicionado.

        Raises:
            ValueError: Se o nome do arquivo não for fornecido.
            IOError: Se ocorrer um erro ao tentar escrever no arquivo.
        """
        compression = self.compression
        if filename is None:
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
        filename: str = '',
        page_number: int = None,
        page_prefix: str = 'Page-',
        target_layer: str = None,
        output_name: str = None,
        date: bool = False,
     ):
        """
        Gera o caminho do arquivo de saída com base nos parâmetros fornecidos, incluindo
        a fonte, o fluxo, o número da página e a camada de destino. O caminho é formatado
        de acordo com a estrutura de diretórios definida para diferentes camadas de dados.

        Args:
            source (str, optional): O identificador da fonte dos dados. O padrão é 'default'.
            stream (str, optional): O identificador do fluxo dos dados. O padrão é 'default'.
            filename (str, optional): O nome do arquivo a ser gerado. Se fornecido, será incluído no caminho.
            page_number (int, optional): O número da página a ser incluído no caminho. Se fornecido, será precedido pelo prefixo.
            page_prefix (str, optional): O prefixo a ser usado antes do número da página. O padrão é 'Page-'.
            target_layer (str, optional): A camada de destino para o arquivo ('raw', 'processing' ou 'staging').
            date (bool, optional): Indica se a data atual deve ser incluída no caminho do arquivo. O padrão é False.

        Returns:
            str: O caminho completo do arquivo de saída gerado com base nos parâmetros fornecidos.
        """
        current_date = Utils.get_current_formatted_date() if date == True else None
        stream_name = output_name if output_name else self.stream

        if target_layer == 'raw':
            target_dir = self._get_raw_dir()
        elif target_layer == 'processing':
            target_dir = self._get_processing_dir()
        elif target_layer == 'staging':
            target_dir = self._get_staging_dir()

        path = f'{target_dir}/{self.source}/{stream_name}'

        if date:
            path += f'/{current_date}'

        if filename:
            path += f'/{filename}'

        return path



    def dump_records(self,
                     records: [list, dict], # type: ignore
                     target_layer = None,
                     file_format: str = '.txt',
                     date:bool=False) -> None:
        """
        Serializa e escreve dados JSON em um arquivo com base no diretório de destino especificado.
        Esta função aceita tanto dicionários quanto listas de dicionários e determina o caminho do arquivo de saída
        com base na camada de destino, nome da source, nome da stream e demais parâmetros.

        Args:
            records (list ou dict): Dados a serem serializados e escritos. Pode ser um dicionário ou uma lista de dicionários.
            target_layer (str): Camada de destino para o arquivo de saída, um de "raw", "processing" ou "staging".
            page_number (int, opcional): Quando fornecido, aplica Número da página a ser incluído no caminho do arquivo de saída.
            file_format (str): A extensão do arquivo de saída. O padrão é '.txt'.
            date (bool): Flag indicando se deve incluir a data atual no nome do arquivo de saída. O padrão é False.

        Raises:
            ValueError: Se o "target_layer" não for um dos valores "raw", "processing" ou "staging".
            DataTypeNotSupportedException: Se os dados fornecidos não forem nem um dicionário nem uma lista.
        """
        if target_layer not in ['raw','processing','staging'] or target_layer is None:
            raise ValueError('"target_layer" must be one of "raw", "processing" or "staging"')

        logger.info(f'Iniciando dump de registros de {self.stream} em {target_layer}')

        output = self.get_output_file_path(
            target_layer=target_layer,
            date=date)

        if isinstance(records, dict):
            rows = [json.dumps(records) + "\n"]
        elif isinstance(records, list):
            rows = [json.dumps(record) + "\n" for record in records]
        else:
            raise DataTypeNotSupportedException(records)

        self._write_row(rows, output, file_format)
        logger.info(f'Dump Finalizado')