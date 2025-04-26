import logging
import subprocess
import json
import os
import yaml
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

    def run_command(
        self,
        command,
        models=None,
        exclude=None,
        selector=None,
        vars_dict=None,
        select=None,
    ):
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

        if select:
            cmd.extend(["--select", select])

        # Log do comando
        logger.info(f"Executando comando dbt: {' '.join(cmd)}")

        try:
            # Executa o comando
            result = subprocess.run(cmd, text=True, capture_output=True)

            # Log da saída
            logger.info(f"Saída do comando dbt:\n{result.stdout}")

            # Verifica se houve erro
            if result.returncode != 0:
                logger.error(
                    f"Comando dbt falhou com código de retorno {result.returncode}"
                )
                logger.error(f"Saída de erro: {result.stderr}")
                return False

            # Retorna sucesso
            return True

        except Exception as e:
            # Log do erro
            logger.error(f"Erro ao executar o comando dbt: {e}")
            return False

    def run(
        self, models=None, exclude=None, selector=None, target_schema=None, select=None
    ):
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
        vars_dict = {"target_schema": target_schema} if target_schema else None
        return self.run_command("run", models, exclude, selector, vars_dict, select)

    def test(
        self, models=None, exclude=None, selector=None, target_schema=None, select=None
    ):
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
        vars_dict = {"target_schema": target_schema} if target_schema else None
        return self.run_command("test", models, exclude, selector, vars_dict, select)

    def build(
        self, models=None, exclude=None, selector=None, target_schema=None, select=None
    ):
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
        vars_dict = {"target_schema": target_schema} if target_schema else None
        return self.run_command("build", models, exclude, selector, vars_dict, select)

    def update_model_configs(self, active_tables, origin):
        """
        Atualiza as configurações dos modelos DBT com base nos metadados das tabelas ativas.

        Estrutura esperada do DataFrame active_tables:
        - active: boolean indicando se o modelo deve estar ativo
        - source_identifier: nome original da fonte de dados na origem
        - unique_id_property: chave primária na origem (apenas para modelos curated)
        - materialization_strategy: estratégia de materialização (apenas para modelos curated)

        Os modelos seguem o padrão de nomenclatura:
        - Modelos processados: bdt_processed_<source_identifier>
        - Modelos curados: bdt_curated_<source_identifier>

        Args:
            active_tables (DataFrame): DataFrame com configurações das tabelas
            origin (str): Origem dos dados (ex: 'bendito', 'notion', etc.)

        Returns:
            bool: True se a atualização foi bem-sucedida, False caso contrário
        """
        logger.info(f"Atualizando configurações de modelos DBT para origem: {origin}")

        try:
            # Caminho para o diretório de modelos da origem
            models_dir = Path(self.project_dir) / "models" / origin
            models_dir.mkdir(parents=True, exist_ok=True)

            # Caminho para o arquivo schema.yml
            schema_path = models_dir / "schema.yml"

            # Estrutura básica do schema.yml
            schema_config = {"version": 2, "models": []}

            # Carregar schema existente, se houver
            if schema_path.exists():
                with open(schema_path, "r") as f:
                    try:
                        existing_schema = yaml.safe_load(f)
                        if existing_schema and "models" in existing_schema:
                            schema_config = existing_schema
                    except Exception as e:
                        logger.warning(
                            f"Erro ao carregar schema existente, criando novo: {e}"
                        )

            # Processando cada tabela ativa
            model_configs = []

            for _, row in active_tables.iterrows():
                source_name = row.get("source_name")
                active = row.get("active", True)

                if source_name:
                    # Configurar modelo processado (sempre view)
                    processed_model_name = f"bdt_processed_{source_name}"
                    processed_config = {
                        "name": processed_model_name,
                        "config": {"materialized": "view", "enabled": bool(active)},
                    }
                    model_configs.append(processed_config)

                    # Configurar modelo curado (pode ser view, table ou incremental)
                    curated_model_name = f"bdt_curated_{source_name}"
                    materialization = row.get("materialization_strategy", "view")
                    unique_key = row.get("unique_id_property")

                    curated_config = {
                        "name": curated_model_name,
                        "config": {
                            "materialized": materialization,
                            "enabled": bool(active),
                        },
                    }

                    # Adiciona unique_key apenas se estiver definido e for incremental
                    if unique_key and materialization == "incremental":
                        curated_config["config"]["unique_key"] = unique_key

                    model_configs.append(curated_config)

            # Atualizar modelos existentes ou adicionar novos
            updated_models = []

            for new_config in model_configs:
                model_name = new_config["name"]

                if existing_model := next(
                    (m for m in schema_config["models"] if m.get("name") == model_name),
                    None,
                ):
                    # Atualizar configuração existente
                    existing_model["config"] = new_config["config"]
                    updated_models.append(existing_model)
                else:
                    # Adicionar nova configuração
                    updated_models.append(new_config)

            # Filtrar apenas os modelos da origem atual
            other_models = [
                m
                for m in schema_config["models"]
                if not (
                    m.get("name", "").startswith("bdt_processed_")
                    or m.get("name", "").startswith("bdt_curated_")
                )
            ]

            # Combinar com outros modelos que não seguem o padrão
            schema_config["models"] = other_models + updated_models

            # Escrever arquivo schema.yml atualizado
            with open(schema_path, "w") as f:
                yaml.dump(schema_config, f, default_flow_style=False, sort_keys=False)

            logger.info(
                f"Configurações de modelos DBT atualizadas com sucesso: {schema_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Erro ao atualizar configurações de modelos DBT: {e}")
            return False
