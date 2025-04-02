import os
import json
import gzip
import logging
from typing import List
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from utils import Utils


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

    def __init__(
        self,
        config: dict,
        source: str = "default",
        stream: str = "default",
        compression: bool = False,
    ) -> None:
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
        self.config = config or {}
        self.compression = compression

        # Create data directories if they don't exist
        base_dir = os.path.join(os.getcwd(), "data")
        for layer in ["raw", "processing", "staging"]:
            dir_path = os.path.join(base_dir, layer)
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Garantindo que o diretório {dir_path} existe.")

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

    def _write_row(
        self, rows: "List[str]", filename: str = None, file_format=None
    ) -> None:
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
            raise ValueError("Invalid filename on _write_row")

        filename = (
            filename + file_format + ".gz" if compression else filename + file_format
        )

        # Ensure directory exists
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            if not compression:
                with open(filename, "wt", encoding="utf-8") as file:
                    file.writelines(rows)
            else:
                with gzip.open(filename, "wt", encoding="utf-8") as file:
                    file.writelines(rows)

            logger.info(f"Successfully wrote to file: {filename}")
        except PermissionError as e:
            logger.error(f"Permission error writing to {filename}: {e}")
            raise Exception(
                f"Cannot write to {filename}. Permission denied. Try using a different directory."
            )
        except OSError as e:
            logger.error(f"OS error writing to {filename}: {e}")
            raise Exception(f"Cannot write to {filename}. OS error: {e}")
        except Exception as e:
            logger.error(f"Error writing to {filename}: {e}")
            raise

    def get_output_file_path(
        self,
        filename: str = "",
        page_number: int = None,
        page_prefix: str = "Page-",
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
        current_date = Utils.get_current_formatted_date() if date else None
        stream_name = output_name or self.stream

        # Use relative paths instead of absolute from config
        base_dir = os.path.join(os.getcwd(), "data")

        if target_layer == "processing":
            target_dir = os.path.join(base_dir, "processing")
        elif target_layer == "raw":
            target_dir = os.path.join(base_dir, "raw")
        elif target_layer == "staging":
            target_dir = os.path.join(base_dir, "staging")
        else:
            target_dir = base_dir

        path = os.path.join(target_dir, self.source, stream_name)

        if date:
            path += f"/{current_date}"

        if filename:
            path += f"/{filename}"

        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        return path

    def dump_records(
        self,
        records: [list, dict],  # type: ignore
        target_layer=None,
        file_format: str = ".txt",
        date: bool = False,
    ) -> None:
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
        if target_layer not in ["raw", "processing", "staging"] or target_layer is None:
            raise ValueError(
                '"target_layer" must be one of "raw", "processing" or "staging"'
            )

        logger.info(f"Iniciando dump de registros de {self.stream} em {target_layer}")

        output = self.get_output_file_path(target_layer=target_layer, date=date)

        if isinstance(records, dict):
            rows = [json.dumps(records) + "\n"]
        elif isinstance(records, list):
            rows = [json.dumps(record) + "\n" for record in records]
        else:
            raise DataTypeNotSupportedException(records)

        try:
            self._write_row(rows, output, file_format)
            logger.info("Dump Finalizado")
        except Exception as e:
            logger.error(f"Erro ao escrever arquivo: {e}")
            raise e

    # def dump_csv(self, df, output_path, sep=';'):
    #     # Convert columns dynamically
    #     converted_df = self.convert_df_int_columns(df)
    #     # Save to CSV
    #     converted_df.to_csv(output_path, index=False, sep=sep, encoding='utf-8')

    # def is_str_col_int(self, value):
    #     if pd.isna(value):  # Check for null values
    #         return True
    #     if isinstance(value, str) and value.endswith('.0'):  # Check if it's a string ending with '.0'
    #         try:
    #             int(float(value))  # Try to convert it to an integer
    #             return True
    #         except ValueError:
    #             return False
    #     return False

    # def convert_df_int_columns(self, df):

    #     #essa função é amaldiçoada e só existe por que o pandas
    #     #decide que é uma boa ideia converter string de int em float quando a coluna tem null.

    #     for col in df.columns:

    #         if pd.api.types.is_integer_dtype(df[col]):
    #             continue

    #         if pd.api.types.is_bool_dtype(df[col]):
    #             # Ensure boolean values are recognized correctly
    #             df[col] = df[col].astype(bool)
    #             continue  # Move to the next column

    #         # Check for float type
    #         if pd.api.types.is_float_dtype(df[col]) and df[col].dropna().apply(lambda x: x.is_integer()).all():
    #             df[col] = df[col].astype('Int64')
    #             continue  # Move to the next column

    #         # Check if the column is of string type (to prevent conversion)
    #         if pd.api.types.is_string_dtype(df[col]):
    #             continue  # Skip string columns entirely

    #         # Check if the column is a datetime type (to prevent conversion)
    #         if pd.api.types.is_datetime64_any_dtype(df[col]):
    #             continue  # Skip datetime columns entirely

    #         # Try to convert non-integer and non-float columns to numeric
    #         if df[col].apply(self.is_str_col_int).all():
    #             df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    #     return df
