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

from bdt_data_integration.src.streams.bitrix_stream import BitrixStream
from bdt_data_integration.src.utils.utils import Utils
from bdt_data_integration.src.utils.notifiers import WebhookNotifier
from bdt_data_integration.src.configuration.configuration import MetadataHandler
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


def main():
    # Parse command line arguments
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

    # Set up the root logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to capture all log messages
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
    )

    # Make sure the bitrix_logger is at the desired level
    bitrix_logger = logging.getLogger("bitrix_logger")
    bitrix_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable postgres_loader logger
    postgres_logger = logging.getLogger("postgres_loader")
    postgres_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

    # Enable stream and extractor loggers with their exact __name__ values
    logging.getLogger("bdt_data_integration.src.stream.bitrix_stream").setLevel(
        logging.DEBUG
    )
    logging.getLogger("bdt_data_integration.src.extractor.bitrix_extractor").setLevel(
        logging.DEBUG
    )
    # Also enable the loggers with __name__ (which would be the module name)
    logging.getLogger("__main__").setLevel(logging.DEBUG)

    logging.getLogger("bdt_data_integration").setLevel(logging.DEBUG)
    # Set the root logger to ensure all child loggers are visible
    logging.getLogger().setLevel(logging.DEBUG)

    load_dotenv()
    origin = "bitrix"
    token = os.environ["BITRIX_TOKEN"]
    bitrix_url = os.environ["BITRIX_URL"]
    bitrix_user_id = os.environ["BITRIX_USER_ID"]
    host = os.environ["DESTINATION_HOST"]
    user = os.environ["DESTINATION_ROOT_USER"]
    password = os.environ["DESTINATION_ROOT_PASSWORD"]
    db_name = os.environ["DESTINATION_DB_NAME"]
    notifier_url = os.environ["MAKE_NOTIFICATION_WEBHOOK"]

    start_time = time.time()
    notifier = WebhookNotifier(url=notifier_url, pipeline="bitrix_pipeline")

    config = Utils.load_config()
    metadata_db_url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
    metadata_engine = create_engine(
        metadata_db_url, poolclass=QueuePool, pool_size=5, max_overflow=10
    )
    metadata_engine = MetadataHandler(metadata_engine, origin)
    df = metadata_engine._load_table_meta()

    # Extracting required information from the DataFrame
    columns_to_fetch = ["table_name", "target_name", "vars"]
    bitrix_data = df[(df["source"] == "bitrix") & (df["active"] == True)][
        columns_to_fetch
    ]

    bitrix_data["mode"] = bitrix_data["vars"].apply(
        lambda x: x.get("type", None) if x else None
    )

    # Selecting and displaying the columns of interest
    active_tables = bitrix_data[["table_name", "target_name", "mode"]].copy()

    @notifier.error_handler
    def replicate_table(origin_table_name, target_table_name, mode="table"):

        logger = logging.getLogger("replicate_database")

        stream = BitrixStream(source_name=origin_table_name, config=config)

        if args.extract.lower() == 'true':
            stream.set_extractor(
                token=token, bitrix_url=bitrix_url, bitrix_user_id=bitrix_user_id
            )
            stream.extract_stream(
                separator=";", start=0, chunksize=args.chunk_size, mode=mode
            )

        if args.load.lower() == 'true':
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {origin}.{target_table_name}(
                "ID" integer NOT NULL,
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
                    target_table=target_table_name,
                    target_schema=origin,
                    chunksize=args.chunk_size,
                    schema_file_type=mode,
                )
            except Exception as e:
                logger.error(f"Erro ao carregar dados: {e}")
                return 0

    base_dir = os.path.join(os.getcwd(), "data")
    dir_path = os.path.join(base_dir, "raw")
    os.makedirs(dir_path, exist_ok=True)
    bitrix_logger.info(f"Garantindo que o diretório {dir_path} existe.")

    total = 0
    success = 0

    if args.table.lower() == "all":
        for i, table in active_tables.iterrows():
            total += 1
            origin_table_name = table["table_name"]
            target_table_name = table["target_name"] or table["table_name"]
            mode = table["mode"]
            metadata_engine.update_table_meta(
                origin_table_name, last_sync_attempt_at=datetime.datetime.now()
            )

            try:
                replicate_table(origin_table_name, target_table_name, mode)
                success += 1
                metadata_engine.update_table_meta(
                    origin_table_name, last_successful_sync_at=datetime.datetime.now()
                )
            except Exception as e:
                success += 0
                raise e

    else:
        origin_table_name = args.table
        target_table_name = f"bdt_raw_{args.table}"
        mode = args.mode
        total += 1
        metadata_engine.update_table_meta(
            origin_table_name, last_sync_attempt_at=datetime.datetime.now()
        )
        try:
            replicate_table(origin_table_name, target_table_name, mode)
            success += 1
            metadata_engine.update_table_meta(
                origin_table_name, last_successful_sync_at=datetime.datetime.now()
            )
        except Exception as e:
            success += 0
            raise e

    # Execute dbt transformations for bitrix models after all tables have been loaded
    logger = logging.getLogger("dbt_runner")
    logger.info("Executando transformações dbt para os modelos do Bitrix")

    if args.transform.lower() == 'true':
        dbt_project_dir = Path(__file__).parent.parent / "dbt"
        dbt_profiles_dir = dbt_project_dir

        # Verificar se o diretório existe
        if not dbt_project_dir.exists():
            logger.error(f"Diretório do projeto dbt não encontrado: {dbt_project_dir}")
        else:
            logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

            # Verificar se existem modelos SQL na pasta bitrix
            bitrix_models_dir = dbt_project_dir / "models" / "bitrix"
            if not bitrix_models_dir.exists():
                logger.error(
                    f"Diretório de modelos bitrix não encontrado: {bitrix_models_dir}"
                )
            else:
                sql_files = list(bitrix_models_dir.glob("*.sql"))
                logger.info(
                    f"Encontrados {len(sql_files)} arquivos SQL no diretório {bitrix_models_dir}"
                )

        # Define o schema de destino para as transformações
        logger.info(f"Usando schema de destino para transformações: {origin}")

        dbt_runner = DBTRunner(
            project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
        )

        # Run only Bitrix models, passando o schema de destino
        dbt_runner.run(models=origin, target_schema=origin)

    notifier = WebhookNotifier(url=notifier_url, pipeline="bitrix_pipeline")

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

    notifier.pipeline_end(
        text=f"Execução de pipeline encerrada: bitrix_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
    )


if __name__ == "__main__":
    main()
