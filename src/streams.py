
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Tuple, Any
from abc import ABC, abstractmethod
from work.bdt_data_integration.src.utils import Utils, WebhookNotifier

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GenericAPIStream(ABC):
    """
    Classe abstrata que define uma de stream de dados e define os métodos obrigatórios.
    Uma Stream é fonte de dados de atualização recorrente, de formatação definida e estrutura estável.
    ex: 
        - Uma tabela em um banco de dados.
        - Um endpoint de listagem de objeto.

    A stream é a classe que gerencia a conexão com tal recurso de forma controlada.
    O retorno de uma stream é um compilado dos dados disponíveis, na sua formatação original
    ex:
        - Uma lista de objetos json com todas as entradas de um sistema.

    """
    def __init__(
        self, identifier: str, base_endpoint, token, writer, auth_method, **kwargs
    ) -> None:
        """
        Init: inicializa um objeto de APIStram da classe genérica

        Kwargs:
        - identifier: string que identifica a API
        - base_endpoint: a parte comum a todas as urls da API
        - token: o token de autenticação utilizado nesta API
        - auth_method: o tipo de autenticação a ser utilizado nesta API
        """
        self.identifier = identifier
        self.token = token
        self.auth_method = auth_method
        self.base_endpoint = base_endpoint

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """
        Método abstrato interno para a obtenção do endpoint da Stream
        """
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> tuple[int, any]:
        """
        Método abstrato interno para a realização de chamadas get
        """
        pass

    @abstractmethod
    def post_data(self, **kwargs) -> tuple[int, any]:
        """
        Método abstrato interno para a realização de chamadas POST
        """
        pass

    @abstractmethod
    
    def fetch_paginated_data(self, **kwargs) -> dict:
        """
        Método abstrato para a implementação da lógica de scroll através de paginações
        """
        pass
    
    @abstractmethod
    def run():
        """
        Método abstrato para a implementação da rotina principal da stream.
        Este método deve retornar todos os dados extraídos da stream, consolidados em um único json.
        """
        pass


class NotionApiStream(GenericAPIStream):
    """
    Stream para a extração de dados da api Database Query do Notion

    Atributos;
        - identifier: notion
        - base_endpoint: 'https://api.notion.com/v1'
        - token: Bearer Token da conta conectada a integração
        - auth_method: o tipo de autorização que será usada na conta, por padrão bearer token.
    """

    def __init__(self, *args, identifier, **kwargs):
        super().__init__(*args, identifier, **kwargs)
        self.identifier = kwargs.get("identifier")
        self.database_id = kwargs.get("database_id")
        self.auth_method = kwargs.get("auth_method")
        self.writer = kwargs.get("writer")

    def _get_endpoint(self) -> str:
        return f"{self.base_endpoint}/databases/{self.database_id}/query"

    def _get_headers(self):
        api_key = self.token
        return {
            "Authorization": f"Bearer {api_key}",
            "Notion-Version": "2021-08-16",
            "Content-Type": "application/json",
            "Data": "{}",
        }

    def _get_next_payload(self, next_cursor=None):
        return {"start_cursor": next_cursor} if next_cursor else {}

    def _extract_next_cursor_from_response(self, response):
        return response.get("next_cursor") if response.get("has_more") else None

    def get_data(self, **kwargs) -> tuple[int, any]:
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        logger.info(f"Gettinng data from endpoint {endpoint}")
        response = requests.get(url=endpoint, headers=headers)
        response.raise_for_status()
        return response.status_code, response.json()

    def post_data(self, json=None, **kwargs) -> tuple[int, any]:
        if json is None:
            json = {}
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        logger.info(f"Gettinng data from endpoint {endpoint}")
        response = requests.post(url=endpoint, headers=headers, json=json)
        response.raise_for_status()

        
        return response.status_code, response.json()

    def fetch_paginated_data(self, **kwargs):
        """
        Método responsável pela obtenção de dados de streams paginadas.

        Args:
            - Endpoint: Obtém o endpoint da stream através do método ._get_endpoint().
            - Headers: Obtém o header das requisições através do método ._get_headers().
        
        Kwargs:
            - Pagination_type: define qual tipo de função de paginação será utilizada,
              por padrão todos os modos são implementados aqui.
        
        Retorno:
            - Devolve um objeto gerador (yield) que pode ser usado para obter as páginas da api.

        """
        pagination_type = kwargs.get("mode","cursor")
        compression = kwargs.get("compression",False)
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        record_list = []
        
        if pagination_type == "cursor":
            successful_requests = 0
            next_cursor = None
            logger.info(f"Attempting scroll fetch from {endpoint}")
            while True:
                payload = self._get_next_payload(next_cursor)
                status_code, response = self.post_data(json=payload)
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
        Método responsável pela ingestão completa da stream.

        Retorno:
            - Um objeto json com todas as páginas da stream combinadas.

        """
        record_list=[]
        date = Utils.get_current_formatted_date()
        for page in self.fetch_paginated_data(mode='cursor'):
            record_list.extend(page)
        self.writer.dump_records(
            records = record_list,
            target_layer='raw',
            date=date)
        return record_list, date


class BenditoAPIStream(GenericAPIStream):
    """
    Stream para a extração de dados da api Database Query do Notion

    Atributos;
        - identifier: notion
        - base_endpoint: 'https://api.notion.com/v1'
        - token: Bearer Token da conta conectada a integração
        - auth_method: o tipo de autorização que será usada na conta, por padrão bearer token.
    """

    def __init__(self, identifier, token, compression=False, **kwargs):
        super().__init__(*args, identifier, **kwargs)
        self.identifier = identifier
        self.token = token
        self.writer = kwargs.get("writer")

    def _get_endpoint(self) -> str:
        return f"https://api-staging.bendito.digital/QueryViews"

    def _get_headers(self):
        api_key = self.token
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
            }

    def fetch_paginated_data(self, query, page_size = 200,  separator = ',', compression = False,):
        """
        Método responsável pela obtenção de dados de streams paginadas.

        Args:
            - Endpoint: Obtém o endpoint da stream através do método ._get_endpoint().
            - Headers: Obtém o header das requisições através do método ._get_headers().
        
        Kwargs:
            - Pagination_type: define qual tipo de função de paginação será utilizada,
              por padrão todos os modos são implementados aqui.
        
        Retorno:
            - Devolve um objeto gerador (yield) que pode ser usado para obter as páginas da api.

        """
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        offset = 0

        while True:
            query_string = f"{query} LIMIT {page_size} OFFSET {offset}"
            payload = json.dumps({"query": query_string, "separator": separator})
            response = requests.post(url=endpoint, headers=headers, data=payload)
            logger.info(response.text)
            if response.status_code == 200:
                if len(response.text.splitlines()) > 1:
                    csv_file = StringIO(response.text)
                    yield pd.read_csv(csv_file, sep=separator)
                    offset += page_size
                else:
                    break
            else:
                logger.error(f'{response.text}')
                break

    def run(self, **kwargs):
        """
        Método responsável pela ingestão completa da stream.

        Retorno:
            - Um objeto json com todas as páginas da stream combinadas.

        """
        all_dataframes = []
        query = kwargs.get('query')
        page_size = kwargs.get('page_size')
        compression = kwargs.get('compression')
        separator = kwargs.get('separator')

        for df in self.fetch_paginated_data(query, page_size,  separator = separator, compression = compression):
            all_dataframes.append(df)
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df