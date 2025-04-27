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

    def update_model_configs(self, tables, origin):
        """
        Updates DBT model configurations based on active tables metadata.

        Args:
            active_tables (DataFrame): Configuration DataFrame containing:
                - active: boolean for model activation status
                - source_name: name of the data source
                - source_identifier: original source identifier
                - unique_id_property: primary key (for curated models)
                - materialization_strategy: materialization strategy (for curated models)
            origin (str): Data origin ('bendito', 'notion', 'bitrix')

        Returns:
            bool: Success status of the update operation
        """
        try:
            models_dir = self._ensure_models_directory(origin)
            schema_config = self._load_existing_schema(models_dir)

            model_configs = self._generate_model_configs(tables)
            updated_schema = self._update_schema_config(
                schema_config, 
                model_configs, 
                origin
            )

            return self._save_schema_config(models_dir, updated_schema)

        except Exception as e:
            logger.error(f"Failed to update DBT model configurations: {e}")
            return False

    def _ensure_models_directory(self, origin):
        """Creates and returns the models directory path."""
        models_dir = Path(self.project_dir) / "models" / origin
        models_dir.mkdir(parents=True, exist_ok=True)
        return models_dir

    def _load_existing_schema(self, models_dir):
        """Loads existing schema.yml if present, or returns default structure."""
        schema_path = models_dir / "schema.yml"
        default_schema = {"version": 2, "models": []}

        if not schema_path.exists():
            return default_schema

        try:
            with open(schema_path, "r") as f:
                existing_schema = yaml.safe_load(f)
                return (
                    existing_schema
                    if existing_schema and "models" in existing_schema
                    else default_schema
                )
        except Exception as e:
            logger.warning(f"Error loading existing schema, creating new: {e}")
            return default_schema

    def _create_model_config(
        self, model_name, materialization, active, unique_key=None
    ):
        """Creates a single model configuration."""
        config = {
            "name": model_name,
            "config": {"materialized": materialization, "enabled": bool(active)},
        }

        if unique_key and materialization == "incremental":
            config["config"]["unique_key"] = unique_key

        return config

    def _generate_model_configs(self, tables):
        """
        Generates configurations for processed and curated models.
        
        Args:
            active_tables (DataFrame): Table configurations
            origin (str): Data origin ('bendito', 'notion', 'bitrix')
        
        Returns:
            list: List of model configurations
        """
        model_configs = []

        for table in tables:
            source_name = table.source_name
            if not source_name:
                continue

            active = table.active

            # Processed model config
            processed_model = self._create_model_config(
                model_name=table.processed_model_name,
                materialization="view",
                active=active,
            )
            model_configs.append(processed_model)

            # Curated model config
            curated_model = self._create_model_config(
                model_name=table.curated_model_name,
                materialization=table.materialization_strategy,
                active=active,
                unique_key=table.unique_id_property,
            )
            model_configs.append(curated_model)

        return model_configs

    def _update_schema_config(self, schema_config, new_configs, origin):
        """
        Updates schema configuration with new model configs.
        
        Args:
            schema_config (dict): Existing schema configuration
            new_configs (list): New model configurations to add/update
            origin (str): Data origin ('bendito', 'notion', 'bitrix')
        
        Returns:
            dict: Updated schema configuration
        """
        suffix = self.get_suffix(origin)
        
        existing_models = {
            model.get("name"): model for model in schema_config["models"]
        }
        updated_models = []

        # Update or add new configurations
        for new_config in new_configs:
            model_name = new_config["name"]
            if model_name in existing_models:
                existing_models[model_name]["config"] = new_config["config"]
                updated_models.append(existing_models[model_name])
            else:
                updated_models.append(new_config)

        # Preserve models from other origins
        other_models = [
            model
            for name, model in existing_models.items()
            if not (
                name.startswith(f"{suffix}_processed_")
                or name.startswith(f"{suffix}_curated_")
            )
        ]

        schema_config["models"] = other_models + updated_models
        return schema_config

    def _save_schema_config(self, models_dir, schema_config):
        """Saves the schema configuration to file."""
        schema_path = models_dir / "schema.yml"
        try:
            with open(schema_path, "w") as f:
                yaml.dump(schema_config, f, default_flow_style=False, sort_keys=False)
            logger.info(f"Successfully updated DBT model configurations: {schema_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save schema configuration: {e}")
            return False
