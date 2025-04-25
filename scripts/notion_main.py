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
    active_tables = handler._load_sync_meta()

    # Define a função que vai processar a extração e o carregamento dos dados
    @notifier.error_handler
    def replicate_database(source_name, identifier, target_name, days):
        # Define logger for this function
        logger = logging.getLogger("replicate_database")

        stream = NotionStream(source_name=source_name, config=config)

        if args.extract.lower() == "true":
            stream.set_extractor(database_id=identifier, token=token)

            records = stream.extract_stream(days=days)

            if args.load.lower() == "true":
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {origin}.{target_name}(
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
                        target_table=target_name,
                        target_schema=origin,
                        chunksize=1000,
                    )
                except Exception as e:
                    logger.error(f"Erro ao carregar dados: {e}")
                    return 0
        elif args.load.lower() == "true":
            logger.info(f"Pulando load em {source_name}: Nada a carregar")
            return 0
        else:
            logger.info(f"Pulando load em {source_name}: Nada a carregar")
            return 0

    total = 0
    success = 0

    if args.extract.lower() == "true" or args.load.lower() == "true":

        if args.table == "all":
            for row in active_tables.itertuples():
                total += 1
                source_name = row.source_name
                target_name = row.target_name or row.source_name
                identifier = row.source_identifier
                days = row.days_interval
                handler.update_table_meta(
                    source_name, last_sync_attempt_at=datetime.datetime.now()
                )

                try:
                    replicate_database(source_name, identifier, target_name, days)
                    success += 1
                    handler.update_table_meta(
                        source_name,
                        last_successful_sync_at=datetime.datetime.now(),
                    )
                except Exception as e:
                    print(f"Error: {e}")
                    success += 0
        elif args.table in active_tables["source_name"].values:
            total += 1
            source_name = args.table
            row = active_tables.loc[active_tables["source_name"] == source_name].iloc[0]
            target_name = row.target_name or source_name
            identifier = row.source_identifier
            handler.update_table_meta(
                source_name, last_sync_attempt_at=datetime.datetime.now()
            )
            try:
                replicate_database(source_name, identifier, target_name)
                success += 1
                handler.update_table_meta(
                    source_name, last_successful_sync_at=datetime.datetime.now()
                )
            except Exception as e:
                print(f"Error: {e}")
                success += 0
        else:
            raise ValueError("Nome da base de dados inválido ou não configurado")

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

    elapsed_time_formatted = Utils.format_elapsed_time(time.time() - start_time)

    if args.silent.lower() == "false":
        notifier.pipeline_end(
            text=f"Execução de pipeline encerrada: notion_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}"
        )


if __name__ == "__main__":
    main()
