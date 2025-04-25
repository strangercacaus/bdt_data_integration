import os
import json
import logging
import requests
import pandas as pd
from io import StringIO

from .base_extractor import GenericAPIExtractor

logger = logging.getLogger(__name__)

class BenditoAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API Database Query do Notion.

    Atributos:
        - identifier (str): Identificador do extrator, neste caso, 'notion'.
        - base_endpoint (str): URL base da API do Notion, 'https://api.notion.com/v1'.
        - token (str): Bearer Token da conta conectada à integração.
    """
    def __init__(self, token, **kwargs):
        """
        Inicializa um extrator para a API do Bendito.

        Args:
            token (str): Token de autenticação para a API Bendito.
            **kwargs: Argumentos nomeados adicionais como 'writer' ou 'schema'.
        """
        # Definir o source diretamente - não usar kwargs para isso
        super().__init__(source='bendito', token=token, **kwargs)
        
        # Não é necessário armazenar token novamente, pois a classe pai já faz isso
        self.schema = kwargs.get('schema', None)

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

    def fetch_paginated(self, query, page_size=200, **kwargs):
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
            
            if response_len < page_size:
                break

    def run(self, **kwargs):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Este método coleta todos os dados paginados e os combina em um único DataFrame.

        Args:
            **kwargs: Argumentos adicionais, incluindo 'query', 'page_size', 'separator' e 'compression'.

        Returns:
            pd.DataFrame: Um DataFrame contendo todos os dados extraídos e combinados com as colunas ID, SUCCESS e CONTENT.
        """
        query = kwargs.get('query','select 1')
        page_size = kwargs.get('page_size',200)
        records = list(
            self.fetch_paginated(
                query,
                page_size,
                separator=kwargs.get('separator', ';'),
                compression=kwargs.get('compression',False)
            )
        )
        df = pd.concat(records, ignore_index=True)
        logger.info(f'{__name__}: Fim da extração.')
        
        # Verificando se existe uma coluna 'id' no DataFrame
        if 'id' in df.columns:
            # Usando a coluna 'id' como ID
            id_column = df['id'].astype(str)
        else:
            # Se não existir coluna 'id', usar o índice
            id_column = df.index.astype(str)
        
        # Abordagem altamente otimizada usando to_json
        # Convertendo o DataFrame para JSON em formato de registros
        json_records = df.to_json(orient='records', lines=True).split('\n')

                # Removendo a linha vazia extra que pode ser gerada
        if json_records and json_records[-1] == '':
            json_records = json_records[:-1]
        
        # Verificando se os tamanhos dos arrays são compatíveis
        if len(json_records) != len(id_column):
            logger.warning(f"Incompatibilidade de tamanho: json_records={len(json_records)}, id_column={len(id_column)}")
            # Ajustando para garantir que os tamanhos sejam iguais
            min_length = min(len(json_records), len(id_column))
            json_records = json_records[:min_length]
            id_column = id_column[:min_length]
        
        
        # Criando o DataFrame de resultado com operações vetorizadas
        result_df = pd.DataFrame({
            "ID": id_column,
            "SUCCESS": True,
            "CONTENT": json_records
        })
        
        return result_df.astype(str) 