import datetime
import json
import os
import yaml
from typing import List
from abc import ABC, abstractmethod


class DataWriter(ABC):
    """
    A classe datawriter centraliza a responsabilidade da escrita de dados no repósitório.
    Também e a responsável por reforçar as regras de negócio de uso das camadas raw, processing e staging,
    através de funções especificas para a manipulação de objetos nestas camadas.
    """

    def __init__(self, resource=None) -> None:
        self.source = source

    @property
    def _get_config():
        with open(f"/work/config/{source}/config.yaml", "r") as file:
            return yaml.safe_load(file)

    @property
    def _get_raw_dir():
        return self.config["RAW_DIR"]

    @property
    def _get_processing_dir():
        return self.config["PROCESSING_DIR"]

    @property
    def _get_staging_dir():
        return self.config["STAGING_DIR"]

    @abstractmethod
    def _write_row(self, row: "str") -> None:
        # os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        # with open(self.filename, "a") as f:
        #     f.write(row)
        pass

    @abstractmethod
    def write(self, data: [List, dict]):
        # if isinstance(data, dict):
        #     self._write_row(json.dumps(data) + "\n")
        # elif isinstance(data, List):
        #     for element in data:
        #         self.write(element)
        # else:
        #     raise DataTypeNotSupportedForIngestionException(data)
        pass
