import os
import time
import logging
import datetime
from dotenv import load_dotenv
import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

sys.path.append(str(Path(__file__).parent.parent.parent))

from bdt_data_integration.src.streams.notion_stream import NotionStream
from bdt_data_integration.src.utils.utils import Utils
from bdt_data_integration.src.utils.notifiers import WebhookNotifier
from bdt_data_integration.src.configuration.configuration import MetadataHandler
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


# Set up the root logger
def main():

    parser = argparse.ArgumentParser(
        description="Run the Bitrix data integration pipeline"
    )

    parser.add_argument(
        "--table",
        type=str,
        default="all",
        help="Specific table to process (default: all tables)",
    )
    parser.add_argument(
        "--database_id",
        type=str,
        default=None,
        help="Specific database_id in notion (default: all tables)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="table",
        help="Specific mode to process (default: all tables)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for data loading (default: 1000)",
    )

    parser.add_argument(
        "--extract",
        type=str,
        default='true',
        choices=['true', 'false'],
        help="Turns data extraction on/off for table (default: True)",
    )

    parser.add_argument(
        "--load",
        type=str,
        default='true',
        choices=['true', 'false'],
        help="Turns data loading on/off for table (default: True)",
    )

    parser.add_argument(
        "--transform",
        type=str,
        default='true',
        choices=['true', 'false'],
        help="Turns data transformation on/off for table (default: True)",
    )
    args = parser.parse_args()

    if args.table != "all" and args.database_id is None:
        raise ValueError("database_id is required when table_name is provided")
    
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,  # Set the minimum logging level (INFO instead of DEBUG to reduce noise)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
    )

    # Make sure the postgres_loader logger is at the desired level
    notion_logger = logging.getLogger("notion_logger")
    notion_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Make sure the postgres_loader logger is at the desired level
    postgres_logger = logging.getLogger("postgres_loader")
    postgres_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable stream and extractor loggers with their exact __name__ values
    logging.getLogger("bdt_data_integration.src.stream.notion_stream").setLevel(
        logging.DEBUG
    )
    logging.getLogger("bdt_data_integration.src.extractor.notion_extractor").setLevel(
        logging.DEBUG
    )
    # Also enable the loggers with __name__ (which would be the module name)
    logging.getLogger("__main__").setLevel(logging.DEBUG)

    logging.getLogger("bdt_data_integration").setLevel(logging.DEBUG)
    # Set the root logger to ensure all child loggers are visible
    logging.getLogger().setLevel(logging.DEBUG)

    load_dotenv()
    origin = "notion"
    token = os.environ["NOTION_APIKEY"]
    database_id = os.environ["NOTION_DATABASE_ID"]
    host = os.environ["DESTINATION_HOST"]
    user = os.environ["DESTINATION_ROOT_USER"]
    password = os.environ["DESTINATION_ROOT_PASSWORD"]
    db_name = os.environ["DESTINATION_DB_NAME"]
    notifier_url = os.environ["DEEPNOTE_BENDITO_BI_WEBHOOK"]

    start_time = time.time()
    notifier = WebhookNotifier(url=notifier_url, pipeline="notion_pipeline")

    config = Utils.load_config()
    metadata_db_url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
    metadata_engine = create_engine(
        metadata_db_url, poolclass=QueuePool, pool_size=5, max_overflow=10
    )
    metadata_engine = MetadataHandler(metadata_engine, origin)
    df = metadata_engine._load_table_meta()

    # Extracting required information from the DataFrame
    columns_to_fetch = ["table_name", "vars", "target_name"]
    notion_data = df[(df["source"] == "notion") & (df["active"] == True)][
        columns_to_fetch
    ]

    notion_data["type"] = notion_data["vars"].apply(lambda x: x.get("type", None))
    notion_data["database_id"] = notion_data["vars"].apply(
        lambda x: x.get("database_id", None)
    )

    # Selecting and displaying the columns of interest
    active_tables = notion_data[["table_name", "type", "database_id", "target_name"]]

    @notifier.error_handler
    def replicate_database(origin_table_name, database_id, target_table_name):
        # Define logger for this function
        logger = logging.getLogger("replicate_database")

        stream = NotionStream(source_name=origin_table_name, config=config)

        if args.extract.lower() == 'true':
            stream.set_extractor(database_id=database_id, token=token)

            stream.extract_stream()

        if args.load.lower() == 'true':
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
                    target_table=target_table_name, target_schema=origin, chunksize=1000
                )
            except Exception as e:
                logger.error(f"Erro ao carregar dados: {e}")
                return 0
            return 0

    base_dir = os.path.join(os.getcwd(), "data")
    dir_path = os.path.join(base_dir, "raw")
    os.makedirs(dir_path, exist_ok=True)
    notion_logger.info(f"Garantindo que o diretório {dir_path} existe.")

    total = 0
    success = 0

    if args.table == "all":
        for i, table in active_tables.iterrows():
            total += 1
            origin_table_name = table["table_name"]
            target_table_name = table["target_name"] or table["table_name"]
            database_id = table["database_id"]
            metadata_engine.update_table_meta(
                origin_table_name, last_sync_attempt_at=datetime.datetime.now()
            )

            try:
                replicate_database(origin_table_name, database_id, target_table_name)
                success += 1
                metadata_engine.update_table_meta(
                    origin_table_name, last_successful_sync_at=datetime.datetime.now()
                )
            except Exception as e:
                print(f"Error: {e}")
                success += 0
    elif args.table != "all" and args.database_id:
        total += 1
        origin_table_name = args.table
        target_table_name = f"ntn_raw_{args.table}"
        database_id = args.database_id
        metadata_engine.update_table_meta(
            origin_table_name, last_sync_attempt_at=datetime.datetime.now()
        )
        try:
            replicate_database(origin_table_name, database_id, target_table_name)
            success += 1
            metadata_engine.update_table_meta(
                origin_table_name, last_successful_sync_at=datetime.datetime.now()
            )
        except Exception as e:
            print(f"Error: {e}")
            success += 0
    else:
        raise ValueError("database_id is required when table_name is provided")

    logger = logging.getLogger("dbt_runner")
    logger.info("Executando transformações dbt para os modelos do Notion")

    if args.transform.lower() == 'true':
        dbt_project_dir = Path(__file__).parent.parent / "dbt"
        dbt_profiles_dir = dbt_project_dir

        # Verificar se o diretório existe
        if not dbt_project_dir.exists():
            logger.error(f"Diretório do projeto dbt não encontrado: {dbt_project_dir}")
        else:
            logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

            # Verificar se existem modelos SQL na pasta notion
            notion_models_dir = dbt_project_dir / "models" / "notion"
            if not notion_models_dir.exists():
                logger.error(
                    f"Diretório de modelos notion não encontrado: {notion_models_dir}"
                )
            else:
                sql_files = list(notion_models_dir.glob("*.sql"))
                logger.info(
                    f"Encontrados {len(sql_files)} arquivos SQL no diretório {notion_models_dir}"
                )

        # Define o schema de destino para as transformações
        logger.info(f"Usando schema de destino para transformações: {origin}")

        dbt_runner = DBTRunner(
            project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
        )

        dbt_runner.run(models="notion", target_schema=origin)

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
    notifier.pipeline_end(
        text=f"Execução de pipeline encerrada: notion_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
    )


if __name__ == "__main__":
    main()
