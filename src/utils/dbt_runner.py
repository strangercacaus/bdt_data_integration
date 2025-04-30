import logging
import subprocess
import json
import yaml
from pathlib import Path
import shutil  # Adicionar import para shutil

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
        # Encontrar o caminho do executável dbt
        self.dbt_path = shutil.which("dbt")

    def get_suffix(self, origin):
        """
        Returns the suffix for a given origin.

        Args:
            origin (str): The origin ('bendito', 'notion', 'bitrix')

        Returns:
            str: The suffix for the origin
        """
        origin_suffixes = {"bendito": "bdt", "notion": "ntn", "bitrix": "btx"}
        if origin not in origin_suffixes:
            raise ValueError(f"Unsupported origin: {origin}")
        return origin_suffixes[origin]

    def run_command(
        self,
        command,
        select=None,
        models=None,
        exclude=None,
        selector=None,
        vars_dict=None,
        full_refresh=False,
    ):
        """
        Executa um comando dbt.

        Args:
            command (str): O comando dbt a ser executado (run, test, build, etc.)
            select (str, optional): String de seleção de modelos
            models (str, optional): String de modelos a serem incluídos
            exclude (str, optional): String de modelos a serem excluídos
            selector (str, optional): Seletor a ser usado
            vars_dict (dict, optional): Variáveis a serem passadas para o dbt
            full_refresh (bool, optional): Se deve fazer um refresh completo

        Returns:
            bool: True se o comando for bem-sucedido, False caso contrário
        """
        # Verifica se o dbt está instalado
        if not self.dbt_path:
            self.dbt_path = shutil.which("dbt")  # Tentar novamente localizar o dbt

        if not self.dbt_path:
            logger.error(
                "Executável dbt não encontrado. Por favor, instale o dbt: pip install dbt-core dbt-postgres"
            )
            return False

        # Prepara o comando base usando o caminho completo do dbt
        cmd = [self.dbt_path, command]

        # Adiciona opções de diretório do projeto
        if self.project_dir:
            cmd.extend(["--project-dir", str(self.project_dir)])

        # Adiciona opções de diretório do profiles
        if self.profiles_dir:
            cmd.extend(["--profiles-dir", str(self.profiles_dir)])

        # Adiciona opções de modelos
        if models:
            cmd.extend(["--models", str(models)])

        # Adiciona opções de exclusão
        if exclude:
            cmd.extend(["--exclude", str(exclude)])

        # Adiciona opções de seletor
        if selector:
            cmd.extend(["--selector", str(selector)])

        # Adiciona variáveis dinâmicas
        if vars_dict:
            # Serializa o dicionário para JSON
            vars_json = json.dumps(vars_dict)
            cmd.extend(["--vars", vars_json])

        if select:
            cmd.extend(["--select", str(select)])

        if full_refresh:
            cmd.extend(["--full-refresh", "true"])

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
        self,
        models=None,
        exclude=None,
        selector=None,
        target_schema=None,
        select=None,
        full_refresh=False,
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
        return self.run_command(
            command="run",
            models=models,
            exclude=exclude,
            selector=selector,
            vars_dict=vars_dict,
            select=select,
            full_refresh=full_refresh,
        )

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
        Rebuilds DBT model configurations completely based on tables metadata.
        Instead of modifying existing schema.yml, this method creates a fresh schema
        based solely on the provided input data.

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
            model_configs = self._generate_model_configs(tables)
            new_schema = {"version": 2, "models": model_configs}

            return self._save_schema_config(models_dir, new_schema)
        except Exception as e:
            logger.error(f"Failed to rebuild DBT model configurations: {e}")
            return False

    def _ensure_models_directory(self, origin):
        """Creates and returns the models directory path."""
        models_dir = Path(self.project_dir) / "models" / origin
        models_dir.mkdir(parents=True, exist_ok=True)
        return models_dir

    def _load_existing_schema(self, models_dir):
        """This method is kept for backward compatibility but is no longer used in the rebuild approach."""
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
            # Processed model config
            if table.active and table.run_dbt_processed == True:
                processed_model = self._create_model_config(
                    model_name=table.processed_model_name,
                    materialization="ephemeral",
                    active=True,
                )
                model_configs.append(processed_model)

            if table.active and table.run_dbt_curated == True:
                curated_model = self._create_model_config(
                    model_name=table.curated_model_name,
                    materialization=table.materialization_strategy,
                    active=False if table.active == False else table.run_dbt_curated,
                    unique_key=table.unique_id_property,
                )
                model_configs.append(curated_model)
        return model_configs

    def _update_schema_config(self, schema_config, new_configs, origin):
        """
        This method is kept for backward compatibility but is no longer used in the rebuild approach.
        In the new approach, we directly create a new schema with only the models from the current origin.

        Args:
            schema_config (dict): Existing schema configuration
            new_configs (list): New model configurations to add/update
            origin (str): Data origin ('bendito', 'notion', 'bitrix')

        Returns:
            dict: Updated schema configuration
        """
        # Simply return a new schema with the provided configurations
        return {"version": 2, "models": new_configs}

    def _save_schema_config(self, models_dir, schema_config):
        """Saves the schema configuration to file."""
        schema_path = models_dir / "schema.yml"
        try:
            # Create a custom dumper class that ignores aliases
            class NoAliasDumper(yaml.Dumper):
                def ignore_aliases(self, data):
                    return True

            with open(schema_path, "w") as f:
                yaml.dump(
                    schema_config,
                    f,
                    Dumper=NoAliasDumper,
                    default_flow_style=False,
                    sort_keys=False,
                )
            logger.info(f"Successfully rebuilt DBT model configurations: {schema_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save schema configuration: {e}")
            return False
