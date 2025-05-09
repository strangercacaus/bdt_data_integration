import logging
from abc import ABC, abstractmethod

from metadata.data_table import DataTable

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Stream(ABC):
    """
    Abstract base class for data streams.
    Defines the interface that all stream classes must implement.
    """

    @abstractmethod
    def __init__(self, table: DataTable):
        """
        Initialize a stream with a source name and configuration.

        Args:
            source_name (str): Name of the source stream
            config (dict): Configuration dictionary
            **kwargs: Additional arguments
        """
        self.table = table
    @abstractmethod
    def set_extractor(self, **kwargs):
        """
        Set up the extractor for this stream.

        Args:
            **kwargs: Arguments specific to the extractor
        """
        pass

    @abstractmethod
    def extract_stream(self, **kwargs) -> None:
        """
        Extract data from the source and write it to the raw layer.

        Args:
            **kwargs: Additional arguments for extraction
        """
        pass

    @abstractmethod
    def set_loader(self, **kwargs):
        """
        Set up the loader for this stream.

        Args:
            **kwargs: Arguments specific to the loader
        """
        pass

    @abstractmethod
    def load_stream(self, **kwargs):
        """
        Load the staged data into the target database.

        Args:
            **kwargs: Additional arguments for loading
        """
        pass
