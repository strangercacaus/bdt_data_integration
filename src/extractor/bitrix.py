import logging
import requests
import pandas as pd
from .base import GenericAPIExtractor

logger = logging.getLogger(__name__)

class BitrixAPIExtractor(GenericAPIExtractor):
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
        super().__init__(*args, **kwargs)
        self.source = 'bitrix'
        self.token = kwargs.get('token')
        self.writer = kwargs.get('writer') 
        self.bitrix_url = kwargs.get('bitrix_url')
        self.bitrix_user_id = kwargs.get('bitrix_user_id')
    
    @property
    def _class_mapping(self):
        return {
            'integer': 'text',
            'double': 'text',
            'char': 'text',
            'enumeration': 'text',
            'datetime': 'text',
            'file': 'text',
        }

    def _list_url(self, endpoint_id):
        return f'https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/crm.{endpoint_id}.list.json'

    def _fields_url(self, endpoint_id):
        return f'https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/crm.{endpoint_id}.fields.json'
    
    def _get_url(self, endpoint_id, object_id):
        return f'https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/crm.{endpoint_id}.get.json?ID={object_id}'

    def _get_endpoint(self, endpoint_id) -> str:
        url = f"https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/{endpoint_id}"
        logger.info(url)
        return url 

    def _build_schema(self):
        pass

    def fetch_paginated(self, url, start=0, **kwargs):
        start = 0
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
    
    def extract_schema(self, endpoint_id, **kwargs):
        url = self._fields_url(endpoint_id)
        records = []
        for response in self.fetch_paginated(url):
            transformed_records = []
            for key, record in response.items():
                record['name'] = key
                transformed_records.append(record)
            records.extend(transformed_records)
        df = pd.DataFrame(transformed_records)
        df = df[['name', 'type']]
        df.columns = ['column_name', 'origin_type']
        df['destination_type'] = df['origin_type'].apply(lambda val: self._class_mapping.get(val, 'text'))
        return Schema(df)

    def extract_list_data(self, endpoint_id, **kwargs):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Este método coleta todos os dados paginados e os combina em um único DataFrame.

        Args:
            **kwargs: Argumentos adicionais, incluindo 'query', 'page_size', 'separator' e 'compression'.

        Returns:
            pd.DataFrame: Um DataFrame contendo todos os dados extraídos e combinados.
        """
        records = []
        url = self._list_url(endpoint_id)
        start = kwargs.get('start',0)
        separator = kwargs.get('separator',';')

        for response in self.fetch_paginated(url, start=0):
            if isinstance(response,list):
                if len(response) > 0:
                    if isinstance(response[0],dict):
                        records.extend(response)
                    else:
                        raise Exception('Invalid Result Format')
                else:
                    logger.info(f'Nenhum dado foi encontrado em {endpoint_id}')
                    break
            else:
                print(type(response))
                raise Exception('Invalid Result Format')
        return pd.DataFrame(records, dtype=str)
    
    def run(self, endpoint_id):
        list_data = self.extract_list_data(endpoint_id)
        result_list = []

        # Iterate over the 'ID' column values
        for object_id in list_data['ID']:
            url = self._get_url(endpoint_id, object_id)
            response = requests.get(url)
            result_list.append(response.json()['result'])  # Add the response to result_list
        return pd.DataFrame(result_list, dtype = str) 