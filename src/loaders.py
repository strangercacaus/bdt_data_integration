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
    def __init__(self, user: str, password: str, host: str, db_name: str):
        self.user = user
        self.password = password
        self.host = host
        self.db_name = db_name
        self.jdbc_string = f'postgresql://{user}:{password}@{host}/{db_name}?sslmode=require'
        self.engine = create_engine(self.jdbc_string)

    def load_sql_schema(self, target_table: str, target_schema: str):
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
            print(self.db_name)
            terminate_query = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{self.db_name}';"
            connection.execute(terminate_query)
    
    def create_updated_at_trigger(self, target_table, target_schema):
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            create_query = f"CREATE TRIGGER set_updated_at_{target_table} BEFORE UPDATE ON {target_schema}.{target_table} FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();"
            connection.execute(create_query)
    
    def truncate_table(self, target_table: str, target_schema: str):
        with self.engine.connect() as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            truncate_query = f"TRUNCATE TABLE {target_schema}.{target_table};"
            connection.execute(truncate_query)

    def create_update_updated_at_function(self):
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            create_query = f"""CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.__updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ LANGUAGE plpgsql;"""
            connection.execute(create_query)

    def create_constraint(self, target_table: str, target_schema: str, column: str, kind=['primary_key','unique_key']):
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            if kind == 'primary_key':
                create_query = f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}_pk PRIMARY KEY ("{column}");"""
            elif kind == 'unique_key':
                create_query = f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}__unique UNIQUE ("{column}");"""
            connection.execute(create_query)
    
    def create_table(self, target_table: str, target_schema: str):
        sql_command = self.load_sql_schema(target_table, target_schema)
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

    def create_sql_schema(self, target_table: str, target_schema: str, primary_key: str = None, unique_columns: list = None):
        self.create_table(target_table, target_schema)
        if primary_key:
            self.create_constraint(target_table,target_schema,column, kind='primary_key')
        if unique_columns:
            for column in unique_columns:
                self.create_constraint(target_table,target_schema,column, kind='unique_key')

        self.create_update_updated_at_function()
        self.create_updated_at_trigger()

                
    def load_data(self, dataframe: pd.DataFrame, target_table: str, target_schema: str, mode = ['append','replace']):
        self.close_connections()
        tables = inspect(self.engine).get_table_names(schema=target_schema)
        check = target_table in tables
        if check != True:
            self.create_sql_schema(target_table, target_schema)
        with self.engine.connect() as connection:
            if mode == 'replace':          
                self.truncate_table(target_table,target_schema)
            dataframe.to_sql(target_table, con=connection, if_exists='append', schema=target_schema, index=False)