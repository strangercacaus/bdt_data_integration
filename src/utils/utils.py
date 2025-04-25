import os
import logging
import sqlparse

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
    def load_config():
        """
        Retorna a configuração padrão do projeto.
        
        Esta implementação simplificada fornece configurações fixas definidas diretamente no código,
        sem necessidade de arquivos externos ou busca complexa por configurações.
        
        Returns:
            dict: Configurações padrão do projeto.
        """
        # Configuração fixa definida diretamente no código
        config = {

            # Parâmetros de conexão padrão
            'POSTGRES': {
                'pool_size': 5,
                'max_overflow': 10,
                'timeout': 30
            },
            
            # Configurações de logging
            'LOGGING': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
        
        logger.info('Configuração padrão carregada.')
        return config
        
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
        