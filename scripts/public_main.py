import logging
from dotenv import load_dotenv
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from bdt_data_integration.src.utils.dbt_runner import DBTRunner
from bdt_data_integration.src.utils.utils import Utils


def main():

    parser = argparse.ArgumentParser(
        description="Run the Bitrix data integration pipeline"
    )

    parser.add_argument(
        "--table_name",
        type=str,
        default="all",
        help="Specific table to process (default: all tables)",
    )

    parser.add_argument(
        "--silent",
        type=str,
        default="false",
        choices=["true", "false"],
        help="Turns notifcation on / off (default: True)",
    )
    # Set up the root logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to capture all log messages
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
    )

    # Make sure the public_logger is at the desired level
    public_logger = logging.getLogger("public_logger")
    public_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable postgres_loader logger
    postgres_logger = logging.getLogger("postgres_loader")
    postgres_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    logging.getLogger("bdt_data_integration").setLevel(logging.DEBUG)
    # Set the root logger to ensure all child loggers are visible
    logging.getLogger().setLevel(logging.DEBUG)

    load_dotenv()
    source = "public"

    # Execute dbt transformations for bitrix models after all tables have been loaded
    logger = logging.getLogger("dbt_runner")
    logger.info("Executando transformações dbt para os modelos de Public")

    # Use the utility function to get the dbt project directory
    dbt_project_dir = Utils.get_dbt_project_dir()
    dbt_profiles_dir = dbt_project_dir

    # Verificar se o diretório existe
    if not dbt_project_dir.exists():
        logger.error(f"Diretório do projeto dbt não encontrado: {dbt_project_dir}")
    else:
        logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

        # Verificar se existem modelos SQL na pasta public
        public_models_dir = dbt_project_dir / "models" / "public"
        if not public_models_dir.exists():
            logger.error(
                f"Diretório de modelos public não encontrado: {public_models_dir}"
            )
        else:
            sql_files = list(public_models_dir.glob("*.sql"))
            logger.info(
                f"Encontrados {len(sql_files)} arquivos SQL no diretório {public_models_dir}"
            )

    # Define o schema de destino para as transformações
    logger.info(f"Usando schema de destino para transformações: {source}")

    dbt_runner = DBTRunner(
        project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
    )

    # Run only Bitrix models, passando o schema de destino
    dbt_runner.run(select="public", target_schema="public")


if __name__ == "__main__":
    main()
