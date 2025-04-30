import os
import json
import logging
import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

from .base_extractor import GenericAPIExtractor

logger = logging.getLogger(__name__)

load_dotenv()


class BenditoAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API do Bendito.

    Atributos:
        - base_endpoint (str): URL base da API do Bendito.
        - token (str): Token de autenticação para a API Bendito.
    """

    def __init__(self, table):
        """
        Inicializa um extrator para a API do Bendito.
        
        Args:
            table (DataTable): Objeto DataTable com as configurações da fonte
        """
        # Definir o source diretamente - não usar kwargs para isso
        super().__init__(table, token=os.environ.get("BENDITO_BI_TOKEN"))

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint para a consulta da API Bendito.

        Returns:
            str: O endpoint da API para consultas.
        """
        return os.environ["BENDITO_BI_URL"]

    def _get_headers(self):
        """
        Gera os cabeçalhos necessários para as requisições à API Bendito.

        Returns:
            dict: Um dicionário contendo os cabeçalhos de autorização e conteúdo.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def post_data(self, payload):
        """
        Método para enviar dados para a API.

        Args:
            payload: Dados a serem enviados para a API

        Returns:
            Response: Resposta da API
        """
        endpoint = self._get_endpoint()
        headers = self._get_headers()
        response = requests.post(url=endpoint, headers=headers, data=payload)
        if response.status_code != 200:
            logger.error(f"{__name__}: {response.text}")
        return response

    def fetch_paginated(self, query, page_size):
        """
        Obtém dados paginados da API Bendito.

        Este método realiza requisições à API para obter dados em páginas,
        utilizando a lógica de paginação.

        Args:
            query (str): A consulta a ser realizada na API.
            page_size (int): O número de registros por página.

        Yields:
            pd.DataFrame: Um gerador que produz DataFrames com os dados extraídos de cada página.
        """
        offset = 0
        results = 0
        page = 0
        logger.info(f"{__name__}: Obtendo página {page + 1} de {query}")
        while True:
            query_string = f"{query} LIMIT {page_size} OFFSET {offset}"
            payload = json.dumps({"query": query_string, "separator": ";"})
            response = self.post_data(payload)
            if response.json().get("success") == False:
                logger.error(f"{__name__}: {response.json().get('error')}")
                break
            response_text = (
                response.text.replace("\r", "").encode("latin1").decode("utf-8")
            )
            csv_file = StringIO(response_text)
            dataframe = pd.read_csv(csv_file, sep=";", encoding="utf-8", dtype=str)

            response_len = dataframe.shape[0]
            results += response_len

            offset += page_size
            page += 1

            yield dataframe

            if response_len < page_size:
                break

    def get_query(self):
        """
        Builds the SQL query for the Bendito API.
            
        Returns:
            str: SQL query string
        """
        query = f'select * from public."{self.table.source_name}" where true'
        if self.table.days_interval > 0:
            query += f" and {self.table.updated_at_property} >= current_date - {self.table.days_interval} days"
        query += " order by 1 asc"
        return query

    def run(self, page_size=1000):
        """
        Executa a rotina principal do extrator, consolidando os dados extraídos.

        Args:
            page_size (int): Page size for pagination

        Returns:
            pd.DataFrame: DataFrame with columns ID, SUCCESS, and CONTENT
        """
        query = self.get_query()
        records = list(
            self.fetch_paginated(query, page_size)
        )

        if not records:
            return pd.DataFrame(columns=["ID", "SUCCESS", "CONTENT"])
            
        df = pd.concat(records, ignore_index=True)

        logger.info(f"{__name__}: Fim da extração.")

        # Verificando se existe uma coluna 'id' no DataFrame
        if "id" in df.columns:
            # Usando a coluna 'id' como ID
            id_column = df["id"].astype(str)
        else:
            # Se não existir coluna 'id', usar o índice
            id_column = df.index.astype(str)

        # Abordagem altamente otimizada usando to_json
        # Convertendo o DataFrame para JSON em formato de registros
        json_records = df.to_json(orient="records", lines=True).split("\n")

        # Removendo a linha vazia extra que pode ser gerada
        if json_records and json_records[-1] == "":
            json_records = json_records[:-1]

        # Verificando se os tamanhos dos arrays são compatíveis
        if len(json_records) != len(id_column):
            logger.warning(
                f"Incompatibilidade de tamanho: json_records={len(json_records)}, id_column={len(id_column)}"
            )
            # Ajustando para garantir que os tamanhos sejam iguais
            min_length = min(len(json_records), len(id_column))
            json_records = json_records[:min_length]
            id_column = id_column[:min_length]

        # Criando o DataFrame de resultado com operações vetorizadas
        result_df = pd.DataFrame(
            {"ID": id_column, "SUCCESS": True, "CONTENT": json_records}
        )

        return result_df.astype(str)
