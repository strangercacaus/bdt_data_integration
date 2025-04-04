import json
import logging
import requests
import pandas as pd
from bdt_data_integration.src.extractors.base_extractor import GenericAPIExtractor
from utils import Schema

logger = logging.getLogger(__name__)  # This will also use the module's name


class BitrixAPIExtractor(GenericAPIExtractor):
    """
    Extrator para a extração de dados da API do Bitrix24.

    Atributos:
        - token (str): Token da API Bitrix.
        - bitrix_url (str): URL da instância Bitrix.
        - bitrix_user_id (str): ID do usuário Bitrix.
    """

    def __init__(self, *args, **kwargs):
        """
        Inicializa um extrator para a API do Bitrix24.

        Args:
            *args: Argumentos posicionais para a classe pai.
            **kwargs: Argumentos nomeados, incluindo 'token', 'bitrix_url', 'bitrix_user_id' e 'writer'.
        """
        # Forçar que o source seja sempre 'bitrix', independente do que foi passado
        kwargs["source"] = "bitrix"
        super().__init__(*args, **kwargs)
        self.token = kwargs.get("token")
        self.writer = kwargs.get("writer")
        self.bitrix_url = kwargs.get("bitrix_url")
        self.bitrix_user_id = kwargs.get("bitrix_user_id")
        
    def _get_endpoint(self):
        return None
    
    def _base_endpoint(self):
        return f"https://{self.bitrix_url}/rest/{self.bitrix_user_id}/{self.token}/"

    def _list_url(self, endpoint_id):
        return self._base_endpoint() + f"{endpoint_id}.list.json"

    def _get_url(self, endpoint_id, object_id):
        return self._base_endpoint() + f"{endpoint_id}.get.json?ID={object_id}"

    def _raw_url(self, endpoint_id) -> str:
        return self._base_endpoint() + f"{endpoint_id}"

    def fetch_paginated(self, url, start=0, **kwargs):
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

    def fetch_list(self, endpoint_id, **kwargs):

        records = []
        url = self._list_url(endpoint_id)

        for response in self.fetch_paginated(url, start=0):
            if isinstance(response, list):
                if len(response) > 0:
                    if isinstance(response[0], dict):
                        records.extend(response)
                    else:
                        raise Exception("Invalid Result Format")
                else:
                    logger.info(f"Nenhum dado foi encontrado em {endpoint_id}")
                    break
            else:
                print(type(response))
                raise Exception("Invalid Result Format")
        return pd.DataFrame(records, dtype=str)

    def extract_as_table(self, endpoint_id):
        list_data = self.fetch_list(endpoint_id)
        if list_data.empty:
            return pd.DataFrame(columns=["ID", "SUCCESS", "CONTENT"])
        results = pd.DataFrame(list_data["ID"])
        results["SUCCESS"] = None
        results["CONTENT"] = None

        for i, object_id in enumerate(results["ID"]):
            url = self._get_url(endpoint_id, object_id)
            response = requests.get(url)

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

    def extract_as_enum(self, endpoint_id):
        url = self._raw_url(endpoint_id)
        response = requests.get(url)
        response.raise_for_status()
        if response.json().get("result"):
            results = [
                {"ID": result.get('ID') or i, "SUCCESS": True, "CONTENT": json.dumps(result)}
                for i, result in enumerate(response.json()["result"])
            ]
            return pd.DataFrame(results)
        else:
            logger.warning(f"WARNING: Nenhum dado presente em {url}")
            return pd.DataFrame(columns=["ID", "SUCCESS", "CONTENT"],dtype=str)

    def extract_as_list(self, endpoint_id):
        list_data = self.fetch_list(endpoint_id)
        
        # Converter o DataFrame inteiro para JSON com tratamento de NaN
        list_json = list_data.replace({pd.NA: None}).to_json(orient='records')
        # Parsear de volta para ter uma lista de dicionários Python
        list_records = json.loads(list_json)
        
        data = [
            {
                "ID": str(record.get("ID", i)),
                "CONTENT": json.dumps(record),
                "SUCCESS": True
            }
            for i, record in enumerate(list_records)
        ]
        
        return pd.DataFrame(data, dtype=str)

    # def extract_as_endpoint(self, endpoint_id, **kwargs):
    #     url = self._get_endpoint(endpoint_id)
    #     results = pd.DataFrame(dtype="str")
    #     results["ID"] = None
    #     results["SUCCESS"] = None
    #     results["CONTENT"] = None

    #     for response in self.fetch_paginated(url, start=0):
    #         if isinstance(response, list):
    #             if len(response) > 0:
    #                 if not isinstance(response[0], dict):
    #                     raise Exception(
    #                         "Formato de resultado inválido (esperado dicionário dentro da lista)"
    #                     )
    #                 for record in response:
    #                     try:
    #                         record_id = record["ID"]
    #                         results.at[0, "ÍD"] = record_id

    #                         if "result" in record:
    #                             results.at[record_id, "CONTENT"] = json.dumps(record)
    #                             results.at[record_id, "SUCCESS"] = True
    #                         else:
    #                             # Caso não tenha a chave 'result', armazena um erro estruturado
    #                             results.at[record_id, "SUCCESS"] = False
    #                             results.at[record_id, "CONTENT"] = json.dumps(
    #                                 {
    #                                     "ERROR": 'Chave "result" não encontrada na resposta',
    #                                     "URL": f"{url}",
    #                                     "STATUS_CODE": response.status_code,
    #                                 }
    #                             )
    #                     except ValueError as e:
    #                         results.at[record_id, "CONTENT"] = json.dumps(
    #                             {
    #                                 "ERROR": "JSON Inválido",
    #                                 "DATA": response,
    #                                 "EXCEPTION": str(e),
    #                             }
    #                         )
    #             else:
    #                 logger.info(f"Nenhum dado foi encontrado em {url}")
    #                 break
    #         else:
    #             print(f"Tipo inesperado de resposta: {type(response)}")
    #             raise Exception("Formato de resultado inválido")

    #     return results

    def get_extract_function(self, mode=("table", "endpoint", "list")):
        # sourcery skip: assign-if-exp, remove-redundant-if
        if mode == "table":
            return self.extract_as_table
        elif mode in ["endpoint", "enum"]:
            return self.extract_as_enum
        elif mode == "list":
            return self.extract_as_list
        else:
            raise ValueError(f"Modo de extração inválido: {mode}")

    def run(self, mode, endpoint_id):
        return self.get_extract_function(mode)(endpoint_id)