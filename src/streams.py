import logging
import datetime
from typing import List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataStream(ABC):
    """
    Lógica de checkpoints, logging de extração, checagem de DQ e Freshness, coisas do tipo.
    """
    def __init__(self,api, writer,endpoint)->None:
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
