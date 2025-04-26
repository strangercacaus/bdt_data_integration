import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class GenericExtractor(ABC):
    """
    Classe abstrata que define um extrator de dados e os métodos obrigatórios.
    """
    def __init__(self, source: str):
        self.source = source

class GenericAPIExtractor(GenericExtractor):
    """
    Classe abstrata que define um extrator de dados e os métodos obrigatórios.
    Um extrator é a interface de extração de uma fonte de dados de atualização recorrente, 
    como por exemplo:
        - Uma tabela em um banco de dados.
        - Um endpoint de listagem de objetos.

    O Extractor gerencia a conexão com tal recurso de forma controlada. 
    O retorno de um Extractor é um compilado dos dados disponíveis, na sua formatação original, 
    como por exemplo:
        - Uma lista de objetos JSON com todas as entradas de um sistema.
    """
    def __init__(
        self, origin: str, token, *args, **kwargs
    ) -> None:
        """
        Inicializa um objeto da classe GenericAPIExtractor.

        Args:
            identifier (str): String que identifica a API.
            token: O token de autenticação utilizado nesta API.
            writer: O objeto responsável por gravar os dados extraídos.
            **kwargs: Argumentos adicionais para configuração do extrator.
        """
        super().__init__(origin, *args, **kwargs)
        self.origin = origin
        self.token = token

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """
        Método abstrato para obter o endpoint do extrator.

        Returns:
            str: O endpoint da API.
        """
        pass

    # @abstractmethod
    # def get_data(self, **kwargs) -> tuple[int, any]:
    #     """
    #     Método abstrato para realizar chamadas GET à API.

    #     Returns:
    #         tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
    #     """
    #     pass

    # @abstractmethod
    # def post_data(self, **kwargs) -> tuple[int, any]:
    #     """
    #     Método abstrato para realizar chamadas POST à API.

    #     Returns:
    #         tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
    #     """
    #     pass

    @abstractmethod
    def fetch_paginated(self, **kwargs) -> dict:
        """
        Método abstrato para implementar a lógica de paginação.

        Returns:
            dict: Um dicionário contendo os dados paginados extraídos.
        """
        pass
    
    @abstractmethod
    def run():
        """
        Método abstrato para implementar a rotina principal do extrator.
        
        Este método deve retornar todos os dados extraídos do extrator, 
        consolidados em um único objeto.

        Returns:
            any: Os dados extraídos consolidados.
        """
        pass 

class GenericDatabaseExtractor(GenericExtractor):
    """
    Classe abstrata que define um extrator de dados e os métodos obrigatórios.
    Um extrator é a interface de extração de uma fonte de dados de atualização recorrente, 
    como por exemplo:
        - Uma tabela em um banco de dados.
        - Um endpoint de listagem de objetos.

    O Extractor gerencia a conexão com tal recurso de forma controlada. 
    O retorno de um Extractor é um compilado dos dados disponíveis, na sua formatação original, 
    como por exemplo:
        - Uma lista de objetos JSON com todas as entradas de um sistema.
    """
    def __init__(
        self, source: str, **kwargs
    ) -> None:
        """
        Inicializa um objeto da classe GenericAPIExtractor.

        Args:
            identifier (str): String que identifica a API.
            token: O token de autenticação utilizado nesta API.
            writer: O objeto responsável por gravar os dados extraídos.
            **kwargs: Argumentos adicionais para configuração do extrator.
        """
        self.source = source

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """
        Método abstrato para obter o endpoint do extrator.

        Returns:
            str: O endpoint da API.
        """
        pass

    # @abstractmethod
    # def get_data(self, **kwargs) -> tuple[int, any]:
    #     """
    #     Método abstrato para realizar chamadas GET à API.

    #     Returns:
    #         tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
    #     """
    #     pass

    # @abstractmethod
    # def post_data(self, **kwargs) -> tuple[int, any]:
    #     """
    #     Método abstrato para realizar chamadas POST à API.

    #     Returns:
    #         tuple[int, any]: Um tupla contendo o código de status e os dados retornados.
    #     """
    #     pass

    @abstractmethod
    def fetch_paginated(self, **kwargs) -> dict:
        """
        Método abstrato para implementar a lógica de paginação.

        Returns:
            dict: Um dicionário contendo os dados paginados extraídos.
        """
        pass
    
    @abstractmethod
    def run():
        """
        Método abstrato para implementar a rotina principal do extrator.
        
        Este método deve retornar todos os dados extraídos do extrator, 
        consolidados em um único objeto.

        Returns:
            any: Os dados extraídos consolidados.
        """
        pass 