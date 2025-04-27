import os
import logging
import sqlparse
import argparse

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Utils:
    """
    Classe utilitária que fornece métodos estáticos para operações comuns.

    Esta classe contém métodos para validação de SQL e carregamento de configurações.
    Todos os métodos são estáticos e podem ser chamados sem a necessidade de instanciar a classe.
    """
    @staticmethod
    def validate_sql(query):
        """
        Valida se uma string contém um SQL válido.
        
        Args:
            query (str): A consulta SQL a ser validada.
            
        Returns:
            bool: True se a consulta for válida, False caso contrário.
        """
        try:
            parsed = sqlparse.parse(query)
            return bool(parsed)  # Returns True if there are parsed statements
        except Exception as e:
            return False
    @staticmethod
    def format_elapsed_time(total_seconds):
        """
        Formata um tempo em segundos para o formato HH:MM:SS
        
        Args:
            total_seconds (float): Tempo total em segundos
            
        Returns:
            str: Tempo formatado no formato HH:MM:SS
        """
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        
        return f"{hours}:{minutes:02d}:{seconds:02d}"
        
    @staticmethod
    def get_parser(description="Run data integration pipeline"):
        """
        Cria e configura um parser de argumentos padrão para os scripts de pipeline.
        
        Args:
            description (str): Descrição do parser
            
        Returns:
            argparse.ArgumentParser: Parser configurado com argumentos padrão
        """
        parser = argparse.ArgumentParser(description=description)
        
        # Tabela específica ou todas
        parser.add_argument(
            "--table",
            type=str,
            default="all",
            help="Specific table to process (default: all tables)",
        )
        
        # Tamanho do chunk para inserção
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=1000,
            help="Chunk size for data loading (default: 1000)",
        )
        
        parser.add_argument(
            "--page_size",
            type=int,
            default=5000,
            help="Page size for data extraction (default: 1000)",
        )
        # Controle de extração
        parser.add_argument(
            "--extract",
            type=str,
            default="true",
            choices=["true", "false"],
            help="Turns data extraction on/off for table (default: True)",
        )
        
        # Controle de carregamento
        parser.add_argument(
            "--load",
            type=str,
            default="true",
            choices=["true", "false"],
            help="Turns data loading on/off for table (default: True)",
        )
        
        # Controle de transformação
        parser.add_argument(
            "--transform",
            type=str,
            default="true",
            choices=["true", "false"],
            help="Turns data transformation on/off for table (default: True)",
        )
        
        # Controle de notificações
        parser.add_argument(
            "--silent",
            type=str,
            default="false",
            choices=["true", "false"],
            help="Turns notification on/off (default: False)",
        )
        
        # Controle de atualização de configurações DBT
        parser.add_argument(
            "--update-dbt-config",
            type=str,
            default="true",
            choices=["true", "false"],
            help="Update DBT model configurations before running (default: True)",
        )
        
        return parser
        