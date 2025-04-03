import logging
import requests

from .base_extractor import GenericAPIExtractor

logger = logging.getLogger(__name__)

class NotionDatabaseAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - database_id (str): ID do banco de dados do Notion.
        - token (str): Bearer Token da conta conectada à integração.
    """
    def __init__(self, token, database_id, **kwargs):
        """
        Inicializa um extrator para a API do Notion.

        Args:
            token (str): Token de acesso à API do Notion.
            database_id (str): ID do banco de dados do Notion.
            **kwargs: Argumentos nomeados adicionais.
        """
        # Definir o source diretamente - não usar kwargs para isso
        super().__init__(source='notion', token=token, **kwargs)
        self.base_endpoint = 'https://api.notion.com/v1/databases'
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

    def fetch_paginated(self, **kwargs):
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
        return [record for page in self.fetch_paginated() for record in page] 