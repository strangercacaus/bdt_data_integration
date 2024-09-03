import datetime
from abc import ABC, abstractmethod
from typing import List

from wrk.src.apis import DaySummaryApi


class DataIngestor(ABC):
    """
    Lógica de checkpoints, logging de extração, checagem de DQ e Freshness, coisas do tipo.
    """
    def __init__(self,api, writer,endpoint)->None
    self.api = api
    self.writer = writer

    @abstractmethod
    def get_checkpoint(self):
        pass

    @abstractmethod
    def set_checkpoint(self):
        pass

    @abstractmethod
    def ingest(self):
        pass
