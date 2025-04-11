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

schema = "bendito"
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
columns_to_fetch = ["table_name", "vars"]
bendito_data = df[(df["source"] == "bendito") & (df["active"] == True)][
    columns_to_fetch
]
# Selecting and displaying the columns of interest
active_tables = bendito_data[["table_name"]]

bendito_logger.info(active_tables)

@notifier.error_handler

def replicate_table(table_name):

    logger = logging.getLogger("replicate_database")

    stream = BenditoStream(source_name=table_name, config=config)

    stream.set_extractor(os.environ["BENDITO_BI_TOKEN"])

    stream.extract_stream(separator=";", page_size=5000)

    stream.transform_stream()

    stream.stage_stream()

    stream.set_loader(
        engine=create_engine(
            f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"
        ),
        schema_file_path=schema_file_path,
        schema_file_type="info_schema",
    )
    stream.load_stream(target_schema=schema, source_name=table_name, chunksize=1000)

total = 0
success = 0
for i, table in active_tables.iterrows():
    total += 1
    table_name = table['table_name']
    meta.update_table_meta(table_name, last_sync_attempt_at = datetime.datetime.now())
    try: 
        replicate_table(table_name)
        success += 1
        meta.update_table_meta(table_name, last_successful_sync_at = datetime.datetime.now())
    except Exception as e:
        success += 0
        raise e
end_time = time.time()
total_time = end_time - start_time
elapsed_time = str(datetime.timedelta(seconds=total_time))
elapsed_time  # Returns a string in the format 'H:MM:SS'

# Convert elapsed_time from string format 'H:MM:SS.ssssss' to 'HH:MM:SS'
hours, minutes, seconds = int(float(str(total_time // 3600).zfill(2))), int(float(str((total_time % 3600) // 60).zfill(2))), int(float(str(round(total_time % 60)).zfill(2)))
elapsed_time_formatted = f"{hours}:{minutes}:{seconds}"

# Update the notifier.pipeline_end call with the formatted time
notifier.pipeline_end(text = f'Execução de pipeline encerrada: bendito_pipeline.\nTotal de tabelas programadas para replicação: {total}, tabelas replicadas com sucesso: {success}, tempo de execução: {elapsed_time_formatted}')