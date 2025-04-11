import logging
import os
import subprocess
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class DBTRunner:
    """
    Utilitário para executar comandos dbt a partir do código Python.
    """
    
    def __init__(self, project_dir=None, profiles_dir=None):
        """
        Inicializa o executador de dbt.
        
        Args:
            project_dir (str): Diretório do projeto dbt
            profiles_dir (str): Diretório onde está o profiles.yml
        """
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        
    def run_command(self, command, models=None, exclude=None, selector=None, vars_dict=None):
        """
        Executa um comando dbt.
        
        Args:
            command (str): O comando dbt a ser executado (run, test, build, etc.)
            models (str, optional): String de modelos a serem incluídos
            exclude (str, optional): String de modelos a serem excluídos
            selector (str, optional): Seletor a ser usado
            vars_dict (dict, optional): Variáveis a serem passadas para o dbt
            
        Returns:
            bool: True se o comando for bem-sucedido, False caso contrário
        """
        # Prepara o comando base
        cmd = ["dbt", command]
        
        # Adiciona opções de diretório do projeto
        if self.project_dir:
            cmd.extend(["--project-dir", self.project_dir])
            
        # Adiciona opções de diretório do profiles
        if self.profiles_dir:
            cmd.extend(["--profiles-dir", self.profiles_dir])
            
        # Adiciona opções de modelos
        if models:
            cmd.extend(["--models", models])
            
        # Adiciona opções de exclusão
        if exclude:
            cmd.extend(["--exclude", exclude])
            
        # Adiciona opções de seletor
        if selector:
            cmd.extend(["--selector", selector])
            
        # Adiciona variáveis dinâmicas
        if vars_dict:
            # Serializa o dicionário para JSON
            vars_json = json.dumps(vars_dict)
            cmd.extend(["--vars", vars_json])
            
        # Log do comando
        logger.info(f"Executando comando dbt: {' '.join(cmd)}")
        
        try:
            # Executa o comando
            result = subprocess.run(
                cmd,
                text=True,
                capture_output=True
            )
            
            # Log da saída
            logger.info(f"Saída do comando dbt:\n{result.stdout}")
            
            # Verifica se houve erro
            if result.returncode != 0:
                logger.error(f"Comando dbt falhou com código de retorno {result.returncode}")
                logger.error(f"Saída de erro: {result.stderr}")
                return False
            
            # Retorna sucesso
            return True
            
        except Exception as e:
            # Log do erro
            logger.error(f"Erro ao executar o comando dbt: {e}")
            return False
            
    def run(self, models=None, exclude=None, selector=None, target_schema=None):
        """
        Executa o comando 'dbt run'.
        
        Args:
            models (str, optional): String de modelos a serem incluídos
            exclude (str, optional): String de modelos a serem excluídos
            selector (str, optional): Seletor a ser usado
            target_schema (str, optional): Schema de destino para os modelos
            
        Returns:
            bool: True se o comando for bem-sucedido, False caso contrário
        """
        vars_dict = None
        if target_schema:
            vars_dict = {"target_schema": target_schema}
        return self.run_command("run", models, exclude, selector, vars_dict)
        
    def test(self, models=None, exclude=None, selector=None, target_schema=None):
        """
        Executa o comando 'dbt test'.
        
        Args:
            models (str, optional): String de modelos a serem incluídos
            exclude (str, optional): String de modelos a serem excluídos
            selector (str, optional): Seletor a ser usado
            target_schema (str, optional): Schema de destino para os modelos
            
        Returns:
            bool: True se o comando for bem-sucedido, False caso contrário
        """
        vars_dict = None
        if target_schema:
            vars_dict = {"target_schema": target_schema}
        return self.run_command("test", models, exclude, selector, vars_dict)
        
    def build(self, models=None, exclude=None, selector=None, target_schema=None):
        """
        Executa o comando 'dbt build'.
        
        Args:
            models (str, optional): String de modelos a serem incluídos
            exclude (str, optional): String de modelos a serem excluídos
            selector (str, optional): Seletor a ser usado
            target_schema (str, optional): Schema de destino para os modelos
            
        Returns:
            bool: True se o comando for bem-sucedido, False caso contrário
        """
        vars_dict = None
        if target_schema:
            vars_dict = {"target_schema": target_schema}
        return self.run_command("build", models, exclude, selector, vars_dict) 