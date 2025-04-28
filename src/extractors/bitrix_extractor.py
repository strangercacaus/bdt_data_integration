from datetime import timedelta
import datetime
import json
import logging
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import time
import random
from bdt_data_integration.src.extractors.base_extractor import GenericAPIExtractor

load_dotenv()

logger = logging.getLogger(__name__)  # This will also use the module's name


class BitrixAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API do Bitrix24.

    Atributos:
        - token (str): Token da API Bitrix.
        - bitrix_url (str): URL da instância Bitrix.
        - bitrix_user_id (str): ID do usuário Bitrix.
    """

    def __init__(self, table):
        """
        Inicializa um extrator para a API do Bitrix24.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'token', 'bitrix_url', 'bitrix_user_id' e 'writer'.
        """
        # Forçar que o source seja sempre 'bitrix', independente do que foi passado
        super().__init__(table, token=os.environ.get("BITRIX_TOKEN"))
        self.base_url = os.environ.get("BITRIX_URL")
        self.user_id = os.environ.get("BITRIX_USER_ID")

    def _get_endpoint(self):
        return None

    def _base_endpoint(self):
        return f"https://{self.base_url}/rest/{self.user_id}/{self.token}/"

    def _list_url(self):
        return self._base_endpoint() + f"{self.table.source_identifier}.list.json"

    def _get_url(self, object_id):
        return (
            self._base_endpoint()
            + f"{self.table.source_identifier}.get.json?ID={object_id}"
        )

    def _raw_url(self) -> str:
        return self._base_endpoint() + f"{self.table.source_identifier}"

    def fetch_paginated(self, url, start=0):
        start = 0
        while True:
            params = {"start": start}
            response = requests.get(url, params=params)
            response.raise_for_status
            if response.status_code != 200:
                raise Exception(
                    f"Error: {response.status_code} - {response.text} for {url}"
                )
            data = response.json()
            yield data.get("result")
            if data.get("next"):
                start = data.get("next")
            else:
                break

    def fetch_list(self):
        records = []
        url = self._list_url()
        days = self.table.days_interval
        if days > 0 and self.table.updated_at_property:
            url += f"?FILTER[>{self.table.updated_at_property}]={(datetime.datetime.now() - datetime.timedelta(days)).strftime('%Y-%m-%d')}"

        for response in self.fetch_paginated(url, start=0):
            if isinstance(response, list):
                if len(response) > 0:
                    if isinstance(response[0], dict):
                        records.extend(response)
                    else:
                        raise Exception("Invalid Result Format")
                else:
                    logger.info(
                        f"Nenhum dado foi encontrado em {self.table.source_identifier}"
                    )
                    break
            else:
                print(type(response))
                raise Exception("Invalid Result Format")
        return pd.DataFrame(records, dtype=str)

    def extract_as_table(self):
        list_data = self.fetch_list()
        if list_data.empty:
            return pd.DataFrame(columns=["ID", "SUCCESS", "CONTENT"])
        results = pd.DataFrame(list_data["ID"])
        results["SUCCESS"] = None
        results["CONTENT"] = None

        max_retries = 5
        for i, object_id in enumerate(results["ID"]):
            url = self._get_url(object_id)

            retry_count = 0
            backoff_time = 1  # Initial backoff time in seconds

            while True:
                response = requests.get(url)

                # Check for rate limiting (429) or service unavailable (503)
                if response.status_code in {429, 503}:

                    retry_count += 1
                    if retry_count <= max_retries:
                        # Log the retry attempt
                        logger.warning(
                            f"Received {response.status_code} error. Retrying in {backoff_time} seconds (attempt {retry_count}/{max_retries})..."
                        )
                        time.sleep(backoff_time)
                        # Exponential backoff with jitter
                        backoff_time = min(30, backoff_time * 2) + random.uniform(0, 1)
                        continue
                    else:
                        # Max retries reached - log and continue with the error response
                        logger.error(
                            f"Max retries reached after {response.status_code} errors for URL: {url}"
                        )

                # Break the retry loop once we have a response (either successful or failed after max retries)
                break

            try:
                # Tenta converter a resposta em JSON
                json_data = response.json()

                # Verifica se a chave 'result' existe na resposta JSON
                if "result" in json_data:
                    results.at[i, "CONTENT"] = json.dumps(json_data["result"])
                    results.at[i, "SUCCESS"] = True
                else:
                    # Caso não tenha a chave 'result', armazena um erro estruturado
                    results.at[i, "SUCCESS"] = False
                    results.at[i, "CONTENT"] = json.dumps(
                        {
                            "ERROR": 'Chave "result" não encontrada na resposta',
                            "URL": f"{url}",
                            "STATUS_CODE": response.status_code,
                        }
                    )
            except ValueError:
                # Caso o conteúdo não seja um JSON válido, armazena um erro estruturado
                results.at[i, "CONTENT"] = json.dumps(
                    {"ERROR": "JSON Inválido", "DATA": response.text}
                )
                results.at[i, "SUCCESS"] = False

        return pd.DataFrame(results, dtype=str)

    def extract_as_enum(self):  # We don't need /updated_at here
        url = self._raw_url(self.table.source_identifier)
        response = requests.get(url)
        response.raise_for_status()
        if response.json().get("result"):
            results = [
                {
                    "ID": result.get("ID") or i,
                    "SUCCESS": True,
                    "CONTENT": json.dumps(result),
                }
                for i, result in enumerate(response.json()["result"])
            ]
            return pd.DataFrame(results)
        else:
            logger.warning(f"WARNING: Nenhum dado presente em {url}")
            return pd.DataFrame(columns=["ID", "SUCCESS", "CONTENT"], dtype=str)

    def extract_as_list(self):
        list_data = self.fetch_list()

        # Converter o DataFrame inteiro para JSON com tratamento de NaN
        list_json = list_data.replace({pd.NA: None}).to_json(orient="records")
        # Parsear de volta para ter uma lista de dicionários Python
        list_records = json.loads(list_json)

        data = [
            {
                "ID": str(record.get("ID", i)),
                "CONTENT": json.dumps(record),
                "SUCCESS": True,
            }
            for i, record in enumerate(list_records)
        ]

        return pd.DataFrame(data, dtype=str)

    def extract_as_fields(
        self
    ):  # We don't need days/updated_at here
        url = self._base_endpoint() + "crm." + self.table.source_identifier
        list_data = requests.get(url).json().get("result")

        data = [
            {
                "ID": int(key) if key.isdigit() else i,
                "SUCCESS": True,
                "CONTENT": json.dumps({"ID": key, **list_data[key]}),
            }
            for i, key in enumerate(list_data.keys())
        ]

        return pd.DataFrame(data, dtype=str)

    def get_extract_function(
        self, extraction_stategy=("table", "enum", "endpoint", "list", "fields")
    ):
        # sourcery skip: assign-if-exp, remove-redundant-if
        if extraction_stategy == "table":
            return self.extract_as_table
        elif extraction_stategy in ["endpoint", "enum"]:
            return self.extract_as_enum
        elif extraction_stategy == "list":
            return self.extract_as_list
        elif extraction_stategy == "fields":
            return self.extract_as_fields
        else:
            raise ValueError(f"Modo de extração inválido: {extraction_stategy}")

    def run(self):
        """
        Run the extraction with the specified mode.

        Args:
            endpoint_id: The Bitrix endpoint ID
            mode: Extraction mode ('table', 'enum', 'endpoint', 'list', 'fields')
            days: Number of days to look back (optional)
            updated_at: Name of the updated_at column in Bitrix (optional)

        Returns:
            DataFrame with the extracted data
        """
        # Get the extraction function based on mode
        extract_func = self.get_extract_function(self.table.extraction_strategy)

        # Call the function with appropriate arguments
        return extract_func()
