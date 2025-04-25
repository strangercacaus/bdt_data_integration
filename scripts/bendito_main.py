import os
import time
import logging
import datetime
import argparse
from dotenv import load_dotenv
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

sys.path.append(str(Path(__file__).parent.parent.parent))

from bdt_data_integration.src.streams.bendito_stream import BenditoStream
from bdt_data_integration.src.utils.utils import Utils
from bdt_data_integration.src.utils.notifier import WebhookNotifier
from bdt_data_integration.src.metadata.sync_metadata import SyncMetadataHandler
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run the Bendito data integration pipeline"
    )
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        help="Table for data extraction (default: all)",
    )
    parser.add_argument(
        "--page_size",
        type=int,
        default=5000,
        help="Page size for data extraction (default: 5000)",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5000,
        help="Chunk size for data insertion (default: 5000)",
    )
    parser.add_argument(
        "--extract",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Turns data extraction on/off for table (default: True)",
    )

    parser.add_argument(
        "--load",
        default="true",
        choices=["true", "false"],
        help="Turns data loading on/off for table (default: True)",
    )

    parser.add_argument(
        "--transform",
        default="true",
        choices=["true", "false"],
        help="Turns data transformation on/off for table (default: True)",
    )

    parser.add_argument(
        "--silent",
        type=str,
        default="false",
        choices=["true", "false"],
        help="Turns notifcation on / off (default: True)",
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
    notifier_url = os.environ["DEEPNOTE_BENDITO_BI_WEBHOOK"]

    start_time = time.time()
    notifier = WebhookNotifier(url=notifier_url, pipeline="bendito_pipeline")

    # Carregando configurações globais do projeto
    config = Utils.load_config()

    # Carregando configurações de sincronização das tabelas
    url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
    engine = create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10)
    handler = SyncMetadataHandler(engine, origin)
    df = handler._load_sync_meta()

    # obtendo as informações das tabelas que serão processadas
    columns_to_fetch = ["table_name", "vars", "target_name"]
    bendito_data = df[(df["source"] == "bendito") & (df["active"] == True)][
        columns_to_fetch
    ]

    bendito_data["mode"] = bendito_data["vars"].apply(
        lambda x: x.get("type", None) if x else None
    )
    active_tables = bendito_data[["table_name", "target_name", "mode"]]

    @notifier.error_handler
    def replicate_table(origin_table_name, target_table_name):

        logger = logging.getLogger("replicate_database")

        if args.extract.lower() == "true":

            stream = BenditoStream(source_name=origin_table_name, config=config)

            stream.set_extractor(os.environ["BENDITO_BI_TOKEN"])

            records = stream.extract_stream(separator=";", page_size=args.page_size)

            if args.load.lower() == "true":

                ddl = f"""
                CREATE TABLE IF NOT EXISTS {origin}.{target_table_name}(
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
                        target_table=target_table_name,
                        target_schema=origin,
                        chunksize=args.chunk_size,
                    )
                except Exception as e:
                    logger.error(f"Erro ao carregar dados: {e}")
                    return 0
                return 0
            else:
                logger.info(f"Pulando load em {origin_table_name}")
                return 0
        elif args.load.lower() == "true":
            logger.info(f"Pulando load em {origin_table_name}: Nada a carregar")
            return 0

    base_dir = os.path.join(os.getcwd(), "data")
    dir_path = os.path.join(base_dir, "raw")
    os.makedirs(dir_path, exist_ok=True)
    bendito_logger.info(f"Garantindo que o diretório {dir_path} existe.")

    total = 0
    success = 0

    if args.table == "all":

        for i, table in active_tables.iterrows():
            total += 1
            origin_table_name = table["table_name"]
            target_table_name = table["target_name"] or table["table_name"]
            handler.update_table_meta(
                origin_table_name, last_sync_attempt_at=datetime.datetime.now()
            )

            try:
                replicate_table(origin_table_name, target_table_name)
                success += 1
                handler.update_table_meta(
                    origin_table_name, last_successful_sync_at=datetime.datetime.now()
                )
            except Exception as e:
                success += 0
                raise e

    else:
        total += 1
        origin_table_name = args.table
        target_table_name = f"bdt_raw_{args.table}"
        handler.update_table_meta(
            origin_table_name, last_sync_attempt_at=datetime.datetime.now()
        )
        try:
            replicate_table(origin_table_name, target_table_name)
            success += 1
            handler.update_table_meta(
                origin_table_name, last_successful_sync_at=datetime.datetime.now()
            )
        except Exception as e:
            success += 0
            raise e

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

        dbt_runner.run(models=origin, target_schema=origin)

    end_time = time.time()
    total_time = end_time - start_time
    elapsed_time = str(datetime.timedelta(seconds=total_time))

    # Convert elapsed_time from string format 'H:MM:SS.ssssss' to 'HH:MM:SS'
    hours, minutes, seconds = (
        int(float(str(total_time // 3600).zfill(2))),
        int(float(str((total_time % 3600) // 60).zfill(2))),
        int(float(str(round(total_time % 60)).zfill(2))),
    )
    elapsed_time_formatted = f"{hours}:{minutes}:{seconds}"

    # Update the notifier.pipeline_end call with the formatted time
    if args.silent.lower() == "false":
        notifier.pipeline_end(
            text=f"Execução de pipeline encerrada: bendito_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
        )


if __name__ == "__main__":
    main()
