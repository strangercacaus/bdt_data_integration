import json
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Tuple, Any
from abc import ABC, abstractmethod
from work.bdt_data_integration.src.utils import Utils, WebhookNotifier

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
        self, identifier: str, token, writer, **kwargs
    ) -> None:
        """
        Inicializa um objeto da classe GenericAPIExtractor.

        Args:
            identifier (str): String que identifica a API.
            token: O token de autenticação utilizado nesta API.
            writer: O objeto responsável por gravar os dados extraídos.
            **kwargs: Argumentos adicionais para configuração do extrator.
        """
        self.identifier = identifier
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
        - auth_method (str): O tipo de autorização que será usada na conta, por padrão, bearer token.
    """
    def __init__(self, *args, **kwargs):
        """
        Inicializa um extrator para a API do Notion.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'identifier', 'database_id' e 'writer'.
        """
        super().__init__(*args, **kwargs)
        self.identifier = kwargs.get("identifier")
        self.database_id = kwargs.get("database_id")
        self.writer = kwargs.get("writer")

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint para a consulta do banco de dados do Notion.

        Returns:
            str: O endpoint formatado para a consulta do banco de dados.
        """
        return f"https://api.notion.com/v1/databases/{self.database_id}/query"

    def _get_headers(self):
        """
        Obtém os cabeçalhos necessários para as requisições à API do Notion.

        Returns:
            dict: Um dicionário contendo os cabeçalhos de autorização e conteúdo.
        """
        api_key = self.token
        return {
            "Authorization": f"Bearer {api_key}",
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
        logger.info(f"Extracting data from endpoint {endpoint}")
        response = requests.get(url=endpoint, headers=headers)
        response.raise_for_status()
        return response.status_code, response.json()

    def post_data(self, json=None, **kwargs) -> tuple[int, any]:
        """
        Realiza uma chamada POST à API do Notion.

        Args:
            json (dict, optional): O corpo da requisição em formato JSON.
            **kwargs: Argumentos adicionais para a requisição.

        Returns:
            tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
        """
        if json is None:
            json = {}
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        logger.info(f"Gettinng data from endpoint {endpoint}")
        response = requests.post(url=endpoint, headers=headers, json=json)
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
        pagination_type = kwargs.get("mode","cursor")
        endpoint = self._get_endpoint()
        
        if pagination_type == "cursor":
            successful_requests = 0
            next_cursor = None
            logger.info(f"Attempting scroll fetch from {endpoint}")
            while True:
                payload = self._get_next_payload(next_cursor)
                response = self.post_data(json=payload)
                successful_requests += 1
                logger.info(f"Successfully fetched page {successful_requests}")
                yield response["results"]
                next_cursor = self._extract_next_cursor_from_response(response)
                if not next_cursor:
                    break

        else:
            """
            Implement other pagination methods here
            """
            logger.warning(
                "Tipo de paginação não implementada, consulte o arquivo apis.py para opções de implementação"
            )

    def run(self):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Returns:
            tuple[list, str]: Uma tupla contendo a lista de registros extraídos e a data atual.
        """
        date = Utils.get_current_formatted_date()
        record_list = [record for page in self.fetch_paginated_data(mode='cursor') for record in page]
        self.writer.dump_records(
            records=record_list,
            target_layer='raw',
            date=date
        )
        return record_list, date


class BenditoAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - identifier (str): Identificador do extrator, neste caso, 'notion'.
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - token (str): Bearer Token da conta conectada à integração.
        - auth_method (str): O tipo de autorização que será usada na conta, por padrão, bearer token.
    """

    def __init__(self, *args, **kwargs):
        """
        Inicializa um extrator para a API do Bendito.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'token', 'identifier' e 'writer'.
        """
        super().__init__(*args, **kwargs)
        self.token = kwargs.get('token')
        self.identifier = kwargs.get('identifier')
        self.writer = kwargs.get("writer")

    def get_data(self):
        """
        Método para obter dados da API.

        Este método deve ser implementado para realizar chamadas GET à API.
        
        Returns:
            None: Este método deve ser implementado.
        """
        return None
    
    def post_data(self):
        """
        Método para enviar dados para a API.

        Este método deve ser implementado para realizar chamadas POST à API.
        
        Returns:
            None: Este método deve ser implementado.
        """
        return None

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint para a consulta da API Bendito.

        Returns:
            str: O endpoint da API para consultas.
        """
        return "https://api-staging.bendito.digital/QueryViews"

    def _get_headers(self):
        """
        Gera os cabeçalhos necessários para as requisições à API Bendito.

        Returns:
            dict: Um dicionário contendo os cabeçalhos de autorização e conteúdo.
        """
        api_key = self.token
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
            }


    def fetch_paginated_data(self, query, page_size=200, separator=',', compression=False):
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
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        offset = 0

        while True:
            query_string = f"{query} LIMIT {page_size} OFFSET {offset}"
            payload = json.dumps({"query": query_string, "separator": separator})
            response = requests.post(url=endpoint, headers=headers, data=payload)
            
            if response.status_code != 200:
                logger.error(f'{response.text}')
                break
            
            if len(response.text.splitlines()) <= 1:
                break
            
            csv_file = StringIO(response.text)
            offset += page_size
            yield pd.read_csv(csv_file, sep=separator)

    def run(self, **kwargs):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Este método coleta todos os dados paginados e os combina em um único DataFrame.

        Args:
            **kwargs: Argumentos adicionais, incluindo 'query', 'page_size', 'separator' e 'compression'.

        Returns:
            pd.DataFrame: Um DataFrame contendo todos os dados extraídos e combinados.
        """
        all_dataframes = list(
            self.fetch_paginated_data(
                kwargs.get('query','select 1'),
                kwargs.get('page_size', 200), 
                separator=kwargs.get('separator', ','),
                compression=kwargs.get('compression',False)
            )
        )
        return pd.concat(all_dataframes, ignore_index=True)
