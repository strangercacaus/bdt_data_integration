import warnings
import logging
import os
import psycopg2
from jinja2 import Template


import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PostgresLoader:
    def __init__(self, user: str = '', password: str = '', host: str = '', db_name: str = ''):
        self.user = user,
        self.password = password,
        self.host = host,
        self.db_name = db_name,
        self.jdbc_string = f'postgresql://{user}:{password}@{host}/{db_name}?sslmode=require'
        self.engine = create_engine(self.jdbc_string)

    def load_sql(self, target_table, target_schema='public'):
        with open(f'/work/schema/{target_table}.sql', 'r', encoding='utf-8') as file:
            text = file.read()
            template = Template(text)
        context = {
            'target_table': target_table,
            'target_schema': target_schema,
            }
        return template.render(context)
    
    def close_connections(self):
         with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            terminate_query = f"""
            SELECT
            pg_terminate_backend(pid)
            FROM
            pg_stat_activity
            WHERE
            pid <> pg_backend_pid()
            AND datname = '{{self.db_name}}';
            """
            connection.execute(terminate_query)


    def create_schema(self, target_table, target_schema='public'):
            sql_command = self.load_sql(target_table, target_schema)
            with self.engine.connect() as connection:
                try:
                    connection.execute(sql_command)
                except SQLAlchemyError as e:
                    if isinstance(e.orig, psycopg2.errors.DuplicateTable):
                        logger.warning(f"Table {target_table} already exists.")
                    else:
                        logger.error(f"Error during table creation: {e}")
                else:
                    logger.info(f"Table {target_table} created successfully.")


                
    def load_data(self, dataframe: pd.DataFrame, target_table: str, mode = ['append','replace'], target_schema:str = 'public'):
        self.close_connections()
        tables = inspect(self.engine).get_table_names(schema=target_schema)
        check = target_table in tables
        if check != True:
            self.create_schema(target_table)

        with self.engine.connect() as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            if mode == 'replace':          
                result = connection.execute(text(f"TRUNCATE TABLE {target_schema}.{target_table};"))
            dataframe.to_sql(target_table, con=connection, if_exists='append', schema=target_schema, index=False)