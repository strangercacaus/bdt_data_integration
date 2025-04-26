import os
import time
import logging
import datetime
from dotenv import load_dotenv
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

sys.path.append(str(Path(__file__).parent.parent.parent))

from bdt_data_integration.src.streams.bendito_stream import BenditoStream
from bdt_data_integration.src.utils.utils import Utils
from bdt_data_integration.src.utils.notifier import WebhookNotifier
from metadata.table_configuration import TableConfiguration
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


def main():
    # Create command line argument parser using the utility method
    parser = Utils.get_parser("Run the Bendito data integration pipeline")

    parser.add_argument(
        "--page_size",
        type=int,
        default=5000,
        help="Page size for data extraction (default: 5000)",
    )

    args = parser.parse_args()
    # Set up the root logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to capture all log messages
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
    )

    # Make sure the bendito_logger is at the desired level
    bendito_logger = logging.getLogger("bendito_logger")
    bendito_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable postgres_loader logger
    postgres_logger = logging.getLogger("postgres_loader")
    postgres_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable stream and extractor loggers with their exact __name__ values
    logging.getLogger("bdt_data_integration.src.stream.bendito_stream").setLevel(
        logging.DEBUG
    )
    logging.getLogger("bdt_data_integration.src.extractor.bendito_extractor").setLevel(
        logging.DEBUG
    )
    # Also enable the loggers with __name__ (which would be the module name)
    logging.getLogger("__main__").setLevel(logging.DEBUG)

    logging.getLogger("bdt_data_integration").setLevel(logging.DEBUG)
    # Set the root logger to ensure all child loggers are visible
    logging.getLogger().setLevel(logging.DEBUG)

    load_dotenv()

    origin = "bendito"
    host = os.environ["DESTINATION_HOST"]
    user = os.environ["DESTINATION_ROOT_USER"]
    password = os.environ["DESTINATION_ROOT_PASSWORD"]
    db_name = os.environ["DESTINATION_DB_NAME"]

    # Carregando configurações globais do projeto
    config = Utils.load_config()
    start_time = time.time()

    notifier_url = os.environ["DEEPNOTE_BENDITO_BI_WEBHOOK"]
    notifier = WebhookNotifier(
        url=notifier_url, pipeline="bendito_pipeline", silent=args.silent.lower()
    )

    # Carregando configurações de sincronização das tabelas
    url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
    config_handler = TableConfiguration(url, origin)
    tables = config_handler.get_table_configuration()
    active_tables = tables[tables["active"] == True]

    logger = logging.getLogger("replicate_database")

    @notifier.error_handler
    def replicate_table(row):

        if args.extract.lower() == "true":

            stream = BenditoStream(source_name=row.source_name, config=config)

            stream.set_extractor()

            records = stream.extract_stream(
                source_name=row.source_name,
                days=row.days_interval,
                updated_at_property=row.updated_at_property,
                page_size=args.page_size,
            )

            if args.load.lower() == "true":

                ddl = f"""
                CREATE TABLE IF NOT EXISTS {origin}.{row.target_name}(
                    "ID" varchar NOT NULL,
                    "SUCCESS" bool,
                    "CONTENT" jsonb
                    );"""

                stream.set_table_definition(ddl)

                stream.set_loader(
                    engine=create_engine(
                        f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"
                    )
                )
                try:
                    stream.load_stream(
                        records,
                        target_table=row.target_name,
                        target_schema=origin,
                        chunksize=args.chunk_size,
                    )
                except Exception as e:
                    logger.error(f"Erro ao carregar dados: {e}")
                    return 0
                return 0
            else:
                logger.info(f"Pulando load em {row.target_name}")
                return 0
        elif args.load.lower() == "true":
            logger.info(f"Pulando load em {row.source_name}: Nada a carregar")
            return 0

    total = 0
    success = 0

    if args.table == "all":
        if args.extract.lower() == "true":
            for row in active_tables.itertuples():
                total += 1
                config_handler.update_table_configuration(
                    row.id,
                    row.source_name,
                    last_sync_attempt_at=datetime.datetime.now(),
                )

                try:
                    replicate_table(row)
                    success += 1
                    config_handler.update_table_configuration(
                        row.id,
                        row.source_name,
                        last_successful_sync_at=datetime.datetime.now(),
                    )

                except Exception as e:
                    success += 0
                    raise e
        else:
            logger.info("Pulando extração de dados")

    elif args.table in active_tables["source_name"].values:
        if args.extract.lower() == "true":
            total += 1
            row = active_tables.loc[active_tables["source_name"] == args.table].iloc[0]
            config_handler.update_table_configuration(
                row.id, row.source_name, last_sync_attempt_at=datetime.datetime.now()
            )
            try:
                replicate_table(row)
                success += 1
                config_handler.update_table_configuration(
                    row.id,
                    row.source_name,
                    last_successful_sync_at=datetime.datetime.now(),
                )
            except Exception as e:
                success += 0
                raise e
        else:
            logger.info(f"Pulando extração de dados para a tabela {args.table}")
            logger.info(f"Pulando o carregamento de dados para a tabela {args.table}")
    else:
        raise ValueError("Nome da tabela inválido ou não configurado")

    if args.transform.lower() == "true":

        logger = logging.getLogger("dbt_runner")
        logger.info("Executando transformações dbt para os modelos do Bendito")

        dbt_project_dir = Path(__file__).parent.parent / "dbt"
        dbt_profiles_dir = dbt_project_dir

        # Verificar se o diretório existe
        if not dbt_project_dir.exists():
            logger.error(f"Diretório do projeto dbt não encontrado: {dbt_project_dir}")
        else:
            logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

            # Verificar se existem modelos SQL na pasta notion
            bendito_models_dir = dbt_project_dir / "models" / "bendito"
            if not bendito_models_dir.exists():
                logger.error(
                    f"Diretório de modelos bendito não encontrado: {bendito_models_dir}"
                )
            else:
                sql_files = list(bendito_models_dir.glob("*.sql"))
                logger.info(
                    f"Encontrados {len(sql_files)} arquivos SQL no diretório {bendito_models_dir}"
                )

        # Define o schema de destino para as transformações
        logger.info(f"Usando schema de destino para transformações: {origin}")

        dbt_runner = DBTRunner(
            project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
        )

        # Atualizar configurações dos modelos antes de executar
        if args.update_dbt_config.lower() == "true":
            logger.info("Atualizando configurações dos modelos DBT...")
            dbt_runner.update_model_configs(tables, origin)

        dbt_runner.run(models=origin, target_schema=origin)
    else:
        logger.info("Pulando transformações dbt")

    elapsed_time_formatted = Utils.format_elapsed_time(time.time() - start_time)
    # Update the notifier.pipeline_end call with the formatted time
    notifier.pipeline_end(
        text=f"Execução de pipeline encerrada: bendito_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
    )


if __name__ == "__main__":
    main()
