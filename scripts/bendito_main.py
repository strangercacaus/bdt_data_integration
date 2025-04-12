import os
import time
import logging
import datetime
import pandas as pd
from dotenv import load_dotenv
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# Add parent directory to path to ensure we can import from the root package
sys.path.append(str(Path(__file__).parent.parent.parent))

from bdt_data_integration.src.streams.bendito_stream import BenditoStream
from bdt_data_integration.src.utils.utils import Utils
from bdt_data_integration.src.utils.notifiers import WebhookNotifier
from bdt_data_integration.src.configuration.configuration import MetadataHandler
from bdt_data_integration.src.utils.dbt_runner import DBTRunner


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
# Garantir que os diretórios necessários existam

# Define base path for schema files relative to package base
project_root = Path(__file__).parent.parent.parent
schema_dir = project_root / "schema" / "bendito"
os.makedirs(schema_dir, exist_ok=True)

load_dotenv()

source = "bendito"
host = os.environ["DESTINATION_HOST"]
user = os.environ["DESTINATION_ROOT_USER"]
password = os.environ["DESTINATION_ROOT_PASSWORD"]
db_name = os.environ["DESTINATION_DB_NAME"]
notifier_url = os.environ["MAKE_NOTIFICATION_WEBHOOK"]


start_time = time.time()
notifier = WebhookNotifier(url=notifier_url, pipeline="bendito_pipeline")

schema_file_path = str(schema_dir / 'bendito.public.information_schema.csv')
schema_df = Utils.get_schema('public')
schema_df.to_csv(schema_file_path, index=False, sep=';')
unique_table_names = schema_df['table_name'].unique()

#Carregando configurações
config = Utils.load_config()
metadata_db_url = (
    f"postgresql+psycopg2://{user}:{password}@{host}:5432/bendito_intelligence_metadata"
)
metadata_engine = create_engine(
    metadata_db_url, poolclass=QueuePool, pool_size=5, max_overflow=10
)
meta = MetadataHandler(metadata_engine, schema)
df = meta._load_table_meta()

# Extracting required information from the DataFrame
columns_to_fetch = ["table_name", "vars", "target_table_name"]
bendito_data = df[(df["source"] == "bendito") & (df["active"] == True)][
    columns_to_fetch
]
# Selecting and displaying the columns of interest
active_tables = bendito_data[["table_name"]]

@notifier.error_handler

def replicate_table(source_name, target_table_name):

    logger = logging.getLogger("replicate_database")

    stream = BenditoStream(source_name=source_name, config=config)

    stream.set_extractor(os.environ["BENDITO_BI_TOKEN"])

    stream.extract_stream(separator=";", page_size=5000)
    
    stream.schema = f"""
    CREATE TABLE IF NOT EXISTS {source_name}.{target_table_name}(
        "ID" varchar NOT NULL,
        "SUCCESS" bool,
        "CONTENT" jsonb
        );"""

    stream.set_loader(
        engine=create_engine(
            f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"
        )
    )
    stream.load_stream(target_schema=schema, source_name=table_name, chunksize=1000)

total = 0
success = 0
for i, table in active_tables.iterrows():
# for table in ['client','person','invoice']:
    total += 1
    table_name = table['table_name']
    target_table_name = table['target_table_name']
#    table_name = table
    meta.update_table_meta(table_name, last_sync_attempt_at = datetime.datetime.now())
    try: 
        replicate_table(table_name, target_table_name)
        success += 1
        meta.update_table_meta(table_name, last_successful_sync_at = datetime.datetime.now())
    except Exception as e:
        success += 0
        raise e

logger = logging.getLogger("dbt_runner")
logger.info("Executando transformações dbt para os modelos do Notion")

dbt_project_dir = Path(__file__).parent.parent.parent / "dbt"
dbt_profiles_dir = dbt_project_dir

# Verificar se o diretório existe
if not dbt_project_dir.exists():
    logger.error(f"Diretório do projeto dbt não encontrado: {dbt_project_dir}")
else:
    logger.info(f"Usando diretório de projeto dbt: {dbt_project_dir}")

    # Verificar se existem modelos SQL na pasta notion
    bendito_models_dir = dbt_project_dir / "models" / "bendito"
    if not bendito_models_dir.exists():
        logger.error(f"Diretório de modelos bendito não encontrado: {bendito_models_dir}")
    else:
        sql_files = list(bendito_models_dir.glob("*.sql"))
        logger.info(
            f"Encontrados {len(sql_files)} arquivos SQL no diretório {bendito_models_dir}"
        )

# Define o schema de destino para as transformações
logger.info(f"Usando schema de destino para transformações: {source}")

dbt_runner = DBTRunner(
    project_dir=str(dbt_project_dir), profiles_dir=str(dbt_profiles_dir)
)

dbt_success = dbt_runner.run(models=source, target_schema= source)

end_time = time.time()
total_time = end_time - start_time
elapsed_time = str(datetime.timedelta(seconds=total_time))
elapsed_time  # Returns a string in the format 'H:MM:SS'

# Convert elapsed_time from string format 'H:MM:SS.ssssss' to 'HH:MM:SS'
hours, minutes, seconds = int(float(str(total_time // 3600).zfill(2))), int(float(str((total_time % 3600) // 60).zfill(2))), int(float(str(round(total_time % 60)).zfill(2)))
elapsed_time_formatted = f"{hours}:{minutes}:{seconds}"

# Update the notifier.pipeline_end call with the formatted time
notifier.pipeline_end(text = f'Execução de pipeline encerrada: bendito_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}')