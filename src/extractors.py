import os
import csv
import json
import logging
import requests
import pandas as pd
import numpy as np
from io import StringIO
from typing import Tuple, Any
from abc import ABC, abstractmethod
from src.utils import Utils, WebhookNotifier

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GenericAPIExtractor(ABC):
    """
    Classe abstrata que define um extrator de dados e os métodos obrigatórios.
    Um extrator é a interface de extração de uma fonte de dados de atualização recorrente, 
    como por exemplo:
        - Uma tabela em um banco de dados.
        - Um endpoint de listagem de objetos.

    O Extractor gerencia a conexão com tal recurso de forma controlada. 
    O retorno de um Extractor é um compilado dos dados disponíveis, na sua formatação original, 
    como por exemplo:
        - Uma lista de objetos JSON com todas as entradas de um sistema.
    """
    def __init__(
        self, source: str, token, **kwargs
    ) -> None:
        """
        Inicializa um objeto da classe GenericAPIExtractor.

        Args:
            identifier (str): String que identifica a API.
            token: O token de autenticação utilizado nesta API.
            writer: O objeto responsável por gravar os dados extraídos.
            **kwargs: Argumentos adicionais para configuração do extrator.
        """
        self.source = source
        self.token = token

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """
        Método abstrato para obter o endpoint do extrator.

        Returns:
            str: O endpoint da API.
        """
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> tuple[int, any]:
        """
        Método abstrato para realizar chamadas GET à API.

        Returns:
            tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
        """
        pass

    @abstractmethod
    def post_data(self, **kwargs) -> tuple[int, any]:
        """
        Método abstrato para realizar chamadas POST à API.

        Returns:
            tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
        """
        pass

    @abstractmethod
    
    def fetch_paginated_data(self, **kwargs) -> dict:
        """
        Método abstrato para implementar a lógica de paginação.

        Returns:
            dict: Um dicionário contendo os dados paginados extraídos.
        """
        pass
    
    @abstractmethod
    def run():
        """
        Método abstrato para implementar a rotina principal do extrator.
        
        Este método deve retornar todos os dados extraídos do extrator, 
        consolidados em um único objeto.

        Returns:
            any: Os dados extraídos consolidados.
        """
        pass


class NotionDatabaseAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - identifier (str): Identificador do extrator, neste caso, 'notion'.
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - token (str): Bearer Token da conta conectada à integração.
    """
    def __init__(self, database_id, **kwargs):
        """
        Inicializa um extrator para a API do Notion.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'identifier', 'database_id' e 'writer'.
        """
        super().__init__(database_id, **kwargs)
        self.base_endpoint = 'https://api.notion.com/v1/databases'
        self.source = 'notion'
        self.database_id = database_id
        self.writer = kwargs.get("writer")

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint para a consulta do banco de dados do Notion.

        Returns:
            str: O endpoint formatado para a consulta do banco de dados.
        """
        return f"{self.base_endpoint}/{self.database_id}/query"

    def _get_headers(self):
        """
        Obtém os cabeçalhos necessários para as requisições à API do Notion.

        Returns:
            dict: Um dicionário contendo os cabeçalhos de autorização e conteúdo.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": "2021-08-16",
            "Content-Type": "application/json",
            "Data": "{}",
        }

    def _get_next_payload(self, next_cursor=None):
        """
        Gera o payload para a próxima requisição, incluindo o cursor de início.

        Args:
            next_cursor (str, optional): O cursor para a próxima página de resultados.

        Returns:
            dict: O payload para a próxima requisição.
        """
        return {"start_cursor": next_cursor} if next_cursor else {}

    def _extract_next_cursor_from_response(self, response):
        """
        Extrai o próximo cursor da resposta da API.

        Args:
            response (dict): A resposta da API contendo informações de paginação.

        Returns:
            str: O próximo cursor, se disponível; caso contrário, None.
        """
        return response.get("next_cursor") if response.get("has_more") else None

    def get_data(self, **kwargs) -> tuple[int, any]:
        """
        Realiza uma chamada GET à API do Notion para obter dados.

        Args:
            **kwargs: Argumentos adicionais para a requisição.

        Returns:
            tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
        """
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        logger.info(f"Enviando requisição GET para {endpoint}")
        response = requests.get(url=endpoint, headers=headers)
        response.raise_for_status()
        return response.status_code, response.json()

    def post_data(self, payload=None, **kwargs) -> tuple[int, any]:
        """
        Realiza uma chamada POST à API do Notion.

        Args:
            json (dict, optional): O corpo da requisição em formato JSON.
            **kwargs: Argumentos adicionais para a requisição.

        Returns:
            tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
        """
        if payload is None:
            payload = {}
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        logger.info(f"Enviando requisição POST para {endpoint}")
        response = requests.post(url=endpoint, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()

    def fetch_paginated_data(self, **kwargs):
        """
        Obtém dados paginados da API do Notion.

        Args:
            **kwargs: Argumentos adicionais, incluindo o tipo de paginação.

        Yields:
            dict: Um gerador que produz os resultados de cada página.
        """
        successful_requests = 0
        next_cursor = None
        logger.info(f"Tentando obter dados de {self._get_endpoint()}")
        while True:
                payload = self._get_next_payload(next_cursor)
                response = self.post_data(payload=payload)
                successful_requests += 1
                logger.info(f"Página {successful_requests} obtida.")
                yield response["results"]
                next_cursor = self._extract_next_cursor_from_response(response)
                if not next_cursor:
                    break      

    def run(self):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Returns:
            tuple[list, str]: Uma tupla contendo a lista de registros extraídos e a data atual.
        """
        return [record for page in self.fetch_paginated_data() for record in page]


class BenditoAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - identifier (str): Identificador do extrator, neste caso, 'notion'.
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - token (str): Bearer Token da conta conectada à integração.
    """
    def __init__(self, *args, **kwargs):
        """
        Inicializa um extrator para a API do Bendito.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'token', 'identifier' e 'writer'.
        """
        super().__init__(*args, **kwargs)
        self.source = 'bendito'
        self.token = kwargs.get('token')
        self.writer = kwargs.get('writer')
        self.schema = kwargs.get('schema',None)
    

    def get_data(self):
        """
        Método para obter dados da API.

        Este método deve ser implementado para realizar chamadas GET à API.
        
        Returns:
            None: Este método deve ser implementado.
        """
        return None
    
    def post_data(self, payload):
        """
        Método para enviar dados para a API.

        Este método deve ser implementado para realizar chamadas POST à API.
        
        Returns:
            None: Este método deve ser implementado.
        """
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        response = requests.post(url=endpoint, headers=headers, data=payload)
        if response.status_code != 200:
            logger.error(f'{__name__}: {response.text}')
        return response

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint para a consulta da API Bendito.

        Returns:
            str: O endpoint da API para consultas.
        """
        return os.environ['BENDITO_BI_URL']

    def _get_headers(self):
        """
        Gera os cabeçalhos necessários para as requisições à API Bendito.

        Returns:
            dict: Um dicionário contendo os cabeçalhos de autorização e conteúdo.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
            }

    def fetch_paginated_data(self, query, page_size=200, **kwargs):
        """
        Obtém dados paginados da API Bendito.

        Este método realiza requisições à API para obter dados em páginas, 
        utilizando a lógica de paginação.

        Args:
            query (str): A consulta a ser realizada na API.
            page_size (int, optional): O número de registros por página. Padrão é 200.
            separator (str, optional): O separador a ser utilizado nos dados. Padrão é ','.
            compression (bool, optional): Indica se a compressão deve ser aplicada. Padrão é False.

        Yields:
            pd.DataFrame: Um gerador que produz DataFrames com os dados extraídos de cada página.
        """
        separator = kwargs.get('separator',';')
        offset = 0
        results = 0
        page = 0
        logger.info(f'{__name__}: Obtendo página {page + 1} de {query}')
        while True:

            #logger.info(f'{__name__}: Obtendo página {page + 1} de {query}')

            query_string = f"{query} LIMIT {page_size} OFFSET {offset}"
            payload = json.dumps({"query": query_string, "separator": separator})

            response = self.post_data(payload)

            response_text = response.text.replace('\r','').encode('latin1').decode('utf-8')
            csv_file = StringIO(response_text)
            dataframe = pd.read_csv(csv_file, sep=separator, encoding='utf-8', dtype=str)

            response_len = dataframe.shape[0]
            results += response_len

            offset += page_size
            page += 1

            yield dataframe
            
            #logger.info(f'{__name__}: Registros obtidos na página {page}: {response_len}, registros totais: {results}.')
            if response_len < page_size:
                break

    def run(self, **kwargs):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Este método coleta todos os dados paginados e os combina em um único DataFrame.

        Args:
            **kwargs: Argumentos adicionais, incluindo 'query', 'page_size', 'separator' e 'compression'.

        Returns:
            pd.DataFrame: Um DataFrame contendo todos os dados extraídos e combinados.
        """
        query = kwargs.get('query','select 1')
        page_size = kwargs.get('page_size',200)
        records = list(
            self.fetch_paginated_data(
                query,
                page_size,
                separator=kwargs.get('separator', ';'),
                compression=kwargs.get('compression',False)
            )
        )
        df = pd.concat(records, ignore_index=True)
        logger.info(f'{__name__}: Fim da extração.')

        
        return df

class BitrixAPIExtractor():
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - identifier (str): Identificador do extrator, neste caso, 'notion'.
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - token (str): Bearer Token da conta conectada à integração.
    """
    def __init__(self, *args, **kwargs):
        """
        Inicializa um extrator para a API do Bitrix24.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'token', 'identifier' e 'writer'.
        """
        self.source = 'bitrix'
        self.token = kwargs.get('token')
        self.writer = kwargs.get('writer') 
        self.bitrix_url = kwargs.get('bitrix_url')
        self.bitrix_user_id = kwargs.get('bitrix_user_id')

    def get_data(self, url, params):
        pass
    
    def post_data(self, payload):
        pass

    def _get_endpoint(self, endpoint_id) -> str:
        url = f"https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/{endpoint_id}"
        logger.info(url)
        return url 

    def _get_headers(self):
        pass

    def fetch_paginated_results(self, endpoint_id='str', start=0, **kwargs):
        start = 0
        records = []
        url = self._get_endpoint(endpoint_id)
        while True:
            params = {
                'start': start
                     }
            response = requests.get(url, params=params)
            response.raise_for_status
            if response.status_code == 200:
                data = response.json()
                yield data.get('result')
                if data.get('next'):
                    start = data.get('next')
                else:
                    break
            else:
                raise Exception(f"Error: {response.status_code} - {response.text}")
                break

    def run(self, endpoint_id, **kwargs):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Este método coleta todos os dados paginados e os combina em um único DataFrame.

        Args:
            **kwargs: Argumentos adicionais, incluindo 'query', 'page_size', 'separator' e 'compression'.

        Returns:
            pd.DataFrame: Um DataFrame contendo todos os dados extraídos e combinados.
        """
        records = []
        
        start = kwargs.get('start',0)
        separator = kwargs.get('separator',';')

        for response in self.fetch_paginated_results(endpoint_id, start=0):
            if isinstance(response,list):
                if len(response) > 0:
                    if isinstance(response[0],dict):
                        records.extend(response)
                    else:
                        raise Exception('Invalid Result Format')
                else:
                    logger.info(f'Nenhum dado foi encontrado em {endpoint_id}')
                    break
            elif isinstance(response,dict):
                transformed_records = []
                for key, record in response.items():
                    record['name'] = key
                    transformed_records.append(record)
                records.extend(transformed_records)
            else:
                print(type(response))
                raise Exception('Invalid Result Format')
        return pd.DataFrame(records, dtype=str)
