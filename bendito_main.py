import os
import sys
import json
import time
import logging
import datetime
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from src.stream.notion_stream import BenditoStream
from src.utils.utils import Utils
from src.utils.notifiers import WebhookNotifier
from src.writers import DataWriter
from src.configuration.configuration import MetadataHandler


# Set up the root logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
start_time = time.time()

# Set up the root logger with more detailed configuration
logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level (INFO instead of DEBUG to reduce noise)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Send log output to the console (screen)
)

# Make sure the postgres_loader logger is at the desired level
postgres_logger = logging.getLogger('postgres_loader')
postgres_logger.setLevel(logging.DEBUG)  # Set to DEBUG for maximum visibility

# Garantir que os diretórios necessários existam
os.makedirs("./schema/notion", exist_ok=True)

load_dotenv()
schema = "bendito"
token = os.environ["NOTION_APIKEY"]
database_id = os.environ["NOTION_DATABASE_ID"]
host = os.environ["NEON_HOST"]
user = os.environ["NEON_ROOT_USER"]
password = os.environ["NEON_ROOT_PASSWORD"]
db_name = os.environ["NEON_DB_NAME"]
notifier_url = os.environ["MAKE_NOTIFICATION_WEBHOOK"]


start_time = time.time()
notifier = WebhookNotifier(url=notifier_url, pipeline="notion_pipeline_development")

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
columns_to_fetch = ["table_name", "vars"]
notion_data = df[df["source"] == "notion"][columns_to_fetch]

# Extracting specific values from the 'vars' column
notion_data["type"] = notion_data["vars"].apply(lambda x: x.get("type", None))
notion_data["database_id"] = notion_data["vars"].apply(
    lambda x: x.get("database_id", None)
)

# Selecting and displaying the columns of interest
active_tables = notion_data[["table_name", "type", "database_id"]]


# @notifier.error_handler
def replicate_database(database_name, database_id):
    # Define logger for this function
    logger = logging.getLogger("replicate_database")

    schema_file_path = f"./schema/notion/{database_name}.sql"
    mapping_file_path = f"./schema/notion/{database_name}.json"
    
    # Verificar se os arquivos necessários existem
    if not os.path.exists(schema_file_path):
        logger.error(f"Arquivo de schema não encontrado: {schema_file_path}")
        return 1
        
    if not os.path.exists(mapping_file_path):
        logger.error(f"Arquivo de mapeamento não encontrado: {mapping_file_path}")
        return 1

    stream = NotionStream(source_name=database_name, config=config)

    stream.set_extractor(database_id=database_id, token=token)

    stream.extract_stream()

    stream.transform_stream(entity="pages")

    stream.stage_stream(rename_columns=True, mapping_file_path=mapping_file_path)

    stream.set_loader(
        engine=create_engine(
            f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"
        ),
        schema_file_path=schema_file_path,
        schema_file_type="template",
    )
    try:
        stream.load_stream(target_schema=schema)
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {e}")
        return 0
    return 0


# sourcery skip: convert-to-enumerate, remove-unused-enumerate
success = 0
total = 0

for i, table in active_tables.iterrows():
    total += 1
    database_name = table["table_name"]
    database_id = table["database_id"]
    meta.update_table_meta(database_name, last_sync_attempt_at=datetime.datetime.now())
    try:
        replicate_database(database_name, database_id)
        success += 1
        meta.update_table_meta(
            database_name, last_successful_sync_at=datetime.datetime.now()
        )
    except Exception as e:
        print(f"Error: {e}")
        success += 0

schema_file_path = "./schema/notion/users.sql"
mapping_file_path = "./schema/notion/users.json"

stream = NotionStream(
    source_name = 'users',
    config=config,
    )

stream.transform_stream(
    entity='users',
    source_stream = 'universal_task_database'
    )

stream.stage_stream(
    rename_columns = True,
    mapping_file_path = mapping_file_path
    )

stream.set_loader(
    engine = create_engine(f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"),
    schema_file_path = schema_file_path,
    schema_file_type = 'template'
    )

try:
    stream.load_stream(
        target_schema = schema
    )
    total += 1
    success += 1
except Exception as e:
    print(f"Error: {e}")
    success += 0

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
# notifier.pipeline_end(text = f'Execução de pipeline encerrada: {schema}_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}')
