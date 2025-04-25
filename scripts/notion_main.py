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
from bdt_data_integration.src.utils.notifier import WebhookNotifier
from bdt_data_integration.src.metadata.sync_metadata import SyncMetadataHandler
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


def main():

    # Parser de argumentos para a execução do script
    parser = argparse.ArgumentParser(
        description="Run the Bitrix data integration pipeline"
    )

    # Define uma tabela específica para processar, se não for definido, todas as fontes do notion no bendito_intelligence_metadata serão processadas
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        help="Specific table to process (default: all tables)",
    )

    # Define o tamanho do chunk no df.to_sql para o carregamento de dados, se não for definido, o tamanho padrão será 1000 registros
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for data loading (default: 1000)",
    )

    # Define se os dados serão extraídos da fonte, true por padrão
    parser.add_argument(
        "--extract",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Turns data extraction on/off for table (default: True)",
    )

    # Define se os dados já existentes no diretório /data/raw serão carregados para banco de dados, true por padrão
    parser.add_argument(
        "--load",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Turns data loading on/off for table (default: True)",
    )

    # Define se os modelos de transformação do dbt serão executados, true por padrão
    parser.add_argument(
        "--transform",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Turns data transformation on/off for table (default: True)",
    )

    # Suprime a notificação no final da execução do script, false por padrão
    parser.add_argument(
        "--silent",
        type=str,
        default="false",
        choices=["true", "false"],
        help="Turns notifcation on / off (default: True)",
    )

    args = parser.parse_args()

    # Configura os diferentes níveis de log para o script
    logging.basicConfig(
        level=logging.INFO,  # Set the minimum logging level (INFO instead of DEBUG to reduce noise)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
    )

    notion_logger = logging.getLogger("notion_logger")
    notion_logger.setLevel(logging.DEBUG)

    postgres_logger = logging.getLogger("postgres_loader")
    postgres_logger.setLevel(logging.DEBUG)

    dbt_logger = logging.getLogger("dbt_runner")
    dbt_logger.setLevel(logging.DEBUG)

    logging.getLogger("bdt_data_integration.src.stream.notion_stream").setLevel(
        logging.DEBUG
    )

    logging.getLogger("bdt_data_integration.src.extractor.notion_extractor").setLevel(
        logging.DEBUG
    )

    logging.getLogger("bdt_data_integration.src.loader.postgres_loader").setLevel(
        logging.DEBUG
    )

    logging.getLogger("__main__").setLevel(logging.DEBUG)

    logging.getLogger("bdt_data_integration").setLevel(logging.DEBUG)

    logging.getLogger().setLevel(logging.DEBUG)

    # Carrega as variáveis de ambiente
    load_dotenv()
    origin = "notion"
    token = os.environ["NOTION_APIKEY"]
    host = os.environ["DESTINATION_HOST"]
    user = os.environ["DESTINATION_ROOT_USER"]
    password = os.environ["DESTINATION_ROOT_PASSWORD"]
    db_name = os.environ["DESTINATION_DB_NAME"]
    notifier_url = os.environ["DEEPNOTE_BENDITO_BI_WEBHOOK"]

    start_time = time.time()
    notifier = WebhookNotifier(url=notifier_url, pipeline="notion_pipeline")

    # Carrega algumas configurações globais do projeto, vai ser morto em breve pq ficou inutilizado
    config = Utils.load_config()

    # Cria uma conexão com o banco de dados de metadados e carrega as informações das tabelas que serão processadas
    url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
    engine = create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10)
    handler = SyncMetadataHandler(engine, origin)
    df = handler._load_sync_meta()

    # Manipula o retorno do banco de metadados para obter os campos necessários para a extração dos dados
    columns_to_fetch = ["table_name", "vars", "target_name"]
    notion_data = df[(df["source"] == "notion") & (df["active"] == True)][
        columns_to_fetch
    ]
    notion_data["type"] = notion_data["vars"].apply(lambda x: x.get("type", None))
    notion_data["database_id"] = notion_data["vars"].apply(
        lambda x: x.get("database_id", None)
    )

    # Armazena as tabelas que serão processadas em um dataframe (tabelas com a replicação ativa)
    active_tables = notion_data[["table_name", "type", "database_id", "target_name"]]

    # Define a função que vai processar a extração e o carregamento dos dados
    @notifier.error_handler
    def replicate_database(origin_table_name, database_id, target_table_name):
        # Define logger for this function
        logger = logging.getLogger("replicate_database")

        stream = NotionStream(source_name=origin_table_name, config=config)

        if args.extract.lower() == "true":
            stream.set_extractor(database_id=database_id, token=token)

            records = stream.extract_stream()

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
                        chunksize=1000,
                    )
                except Exception as e:
                    logger.error(f"Erro ao carregar dados: {e}")
                    return 0
        elif args.load.lower() == "true":
            logger.info(f"Pulando load em {origin_table_name}: Nada a carregar")
            return 0

    total = 0
    success = 0

    if args.extract.lower() == "true" or args.load.lower() == "true":

        if args.table == "all":
            for i, table in active_tables.iterrows():
                total += 1
                origin_table_name = table["table_name"]
                target_table_name = table["target_name"] or table["table_name"]
                database_id = table["database_id"]
                handler.update_table_meta(
                    origin_table_name, last_sync_attempt_at=datetime.datetime.now()
                )

                try:
                    replicate_database(
                        origin_table_name, database_id, target_table_name
                    )
                    success += 1
                    handler.update_table_meta(
                        origin_table_name,
                        last_successful_sync_at=datetime.datetime.now(),
                    )
                except Exception as e:
                    print(f"Error: {e}")
                    success += 0
        elif args.table != "all":
            total += 1
            origin_table_name = args.table
            target_table_name = active_tables[
                active_tables["table_name"] == origin_table_name
            ]["target_name"].values[0]
            database_id = active_tables[
                active_tables["table_name"] == origin_table_name
            ]["database_id"].values[0]
            handler.update_table_meta(
                origin_table_name, last_sync_attempt_at=datetime.datetime.now()
            )
            try:
                replicate_database(origin_table_name, database_id, target_table_name)
                success += 1
                handler.update_table_meta(
                    origin_table_name, last_successful_sync_at=datetime.datetime.now()
                )
            except Exception as e:
                print(f"Error: {e}")
                success += 0
        else:
            raise ValueError("database_id is required when table_name is provided")

    if args.transform.lower() == "true":

        dbt_project_dir = Path(__file__).parent.parent / "dbt"
        dbt_profiles_dir = dbt_project_dir

        # Verificar se o diretório existe
        if not dbt_project_dir.exists():
            dbt_logger.error(
                f"Diretório do projeto dbt não encontrado: {dbt_project_dir}"
            )
        else:
            dbt_logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

            # Verificar se existem modelos SQL na pasta notion
            notion_models_dir = dbt_project_dir / "models" / "notion"
            if not notion_models_dir.exists():
                dbt_logger.error(
                    f"Diretório de modelos notion não encontrado: {notion_models_dir}"
                )
            else:
                sql_files = list(notion_models_dir.glob("*.sql"))
                dbt_logger.info(
                    f"Encontrados {len(sql_files)} arquivos SQL no diretório {notion_models_dir}"
                )

        # Define o schema de destino para as transformações
    dbt_logger.info(f"Usando schema de destino para transformações: {origin}")

    dbt_runner = DBTRunner(
        project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
    )

    dbt_runner.run(models="notion", target_schema=origin)

    end_time = time.time()
    total_time = end_time - start_time
    # Convert elapsed_time from string format 'H:MM:SS.ssssss' to 'HH:MM:SS'
    hours, minutes, seconds = (
        int(float(str(total_time // 3600).zfill(2))),
        int(float(str((total_time % 3600) // 60).zfill(2))),
        int(float(str(round(total_time % 60)).zfill(2))),
    )
    elapsed_time_formatted = f"{hours}:{minutes}:{seconds}"

    if args.silent.lower() == "false":
        notifier.pipeline_end(
            text=f"Execução de pipeline encerrada: notion_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
        )


if __name__ == "__main__":
    main()
