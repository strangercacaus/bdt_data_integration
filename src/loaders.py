import os
import logging
import psycopg2
import warnings
import pandas as pd
from jinja2 import Template
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PostgresLoader:
    """
    Classe para carregar dados em um banco de dados PostgreSQL.

    Esta classe fornece métodos para criar tabelas, gerenciar esquemas e carregar dados de DataFrames no PostgreSQL. 
    Também suporta a criação de gatilhos e restrições para manter a integridade dos dados.

    Atributos:
        user (str): O nome de usuário para a conexão com o banco de dados PostgreSQL.
        password (str): A senha para a conexão com o banco de dados PostgreSQL.
        host (str): O endereço do host do banco de dados PostgreSQL.
        db_name (str): O nome do banco de dados PostgreSQL.
        jdbc_string (str): A string de conexão JDBC para o banco de dados PostgreSQL.
        engine: O engine do SQLAlchemy para conectar ao banco de dados PostgreSQL.
    """
    def __init__(self, user: str, password: str, host: str, db_name: str):
        """
        Inicializa o PostgresLoader com os parâmetros de conexão do banco de dados.

        Args:
            user (str): O nome de usuário para a conexão com o banco de dados PostgreSQL.
            password (str): A senha para a conexão com o banco de dados PostgreSQL.
            host (str): O endereço do host do banco de dados PostgreSQL.
            db_name (str): O nome do banco de dados PostgreSQL.
        """
        self.user = user
        self.password = password
        self.host = host
        self.db_name = db_name
        self.jdbc_string = (
            f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require"
        )
        self.engine = create_engine(self.jdbc_string)

    def load_sql_schema(self, target_table: str, target_schema: str):
        """
        Carrega o esquema SQL de uma tabela a partir de um arquivo.

        Este método lê um arquivo SQL correspondente à tabela de destino e renderiza um template 
        com o nome da tabela e do esquema.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela será criada.

        Returns:
            str: O comando SQL renderizado para criar a tabela.
        """
        with open(f"/work/schema/{target_table}.sql", "r", encoding="utf-8") as file:
            text = file.read()
            template = Template(text)
        context = {
            "target_table": target_table,
            "target_schema": target_schema,
        }
        return template.render(context)

    def close_connections(self):
        """
        Fecha as conexões ativas com o banco de dados.

        Este método termina todas as conexões ativas para o banco de dados especificado.
        """
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            terminate_query = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{self.db_name}';"
            connection.execute(terminate_query)

    def create_updated_at_trigger(self, target_table, target_schema):
        """
        Cria um gatilho para atualizar a coluna de data de modificação.

        Este método cria um gatilho que atualiza a coluna de data de modificação 
        sempre que uma linha na tabela é atualizada.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
        """
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            create_query = f"CREATE TRIGGER set_updated_at_{target_table} BEFORE UPDATE ON {target_schema}.{target_table} FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();"
            connection.execute(create_query)

    def truncate_table(self, target_table: str, target_schema: str):
        """
        Trunca a tabela de destino, removendo todos os registros.

        Este método remove todos os registros da tabela especificada, mantendo a estrutura da tabela.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
        """
        with self.engine.connect() as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            truncate_query = f"TRUNCATE TABLE {target_schema}.{target_table};"
            connection.execute(truncate_query)

    def create_update_updated_at_function(self):
        """
        Cria uma função para atualizar a coluna de data de modificação.

        Este método cria ou substitui uma função que define a coluna de data de modificação 
        para o timestamp atual.
        """
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            create_query = "CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.__updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ LANGUAGE plpgsql;"
            connection.execute(create_query)

    def create_constraint(
        self,
        target_table: str,
        target_schema: str,
        column: str,
        kind=("primary_key", "unique_key"),
    ):
        """
        Cria uma restrição de chave primária ou única na tabela de destino.

        Este método adiciona uma restrição de chave primária ou única à coluna especificada 
        na tabela de destino.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
            column (str): O nome da coluna para a qual a restrição será aplicada.
            kind (list, optional): O tipo de restrição a ser criada ('primary_key' ou 'unique_key').
        """
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            if kind == "primary_key":
                create_query = f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}_pk PRIMARY KEY ("{column}");"""
            elif kind == "unique_key":
                create_query = f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}__unique UNIQUE ("{column}");"""
            connection.execute(create_query)

    def create_table(self, target_table: str, target_schema: str):
        """
        Cria a tabela de destino com base no esquema SQL carregado.

        Este método executa o comando SQL para criar a tabela de destino e lida com erros 
        caso a tabela já exista.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela será criada.
        """
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

    def create_sql_schema(
        self,
        target_table: str,
        target_schema: str,
        primary_key: str = None,
        unique_columns: list = None,
    ):
        """
        Cria o esquema SQL para a tabela de destino.

        Este método cria a tabela e, se especificado, adiciona restrições de chave primária 
        e colunas únicas, além de configurar a função e o gatilho para a coluna de data de modificação.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela será criada.
            primary_key (str, optional): O nome da coluna a ser usada como chave primária.
            unique_columns (list, optional): Uma lista de colunas que devem ser únicas.
        """
        self.create_table(target_table, target_schema)
        if primary_key:
            self.create_constraint(
                target_table, target_schema, column, kind="primary_key"
            )
        if unique_columns:
            for column in unique_columns:
                self.create_constraint(
                    target_table, target_schema, column, kind="unique_key"
                )

        self.create_update_updated_at_function()
        self.create_updated_at_trigger()

    def load_data(
        self,
        dataframe: pd.DataFrame,
        target_table: str,
        target_schema: str,
        mode=("append", "replace"),
    ):
        """
        Carrega os dados de um DataFrame na tabela de destino.

        Este método verifica se a tabela existe e, se não existir, cria o esquema SQL. 
        Em seguida, carrega os dados do DataFrame na tabela, podendo substituir os dados existentes.

        Args:
            dataframe (pd.DataFrame): O DataFrame contendo os dados a serem carregados.
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
            mode (tuple, optional): O modo de carregamento ('append' para adicionar, 'replace' para substituir).
        """
        self.close_connections()
        tables = inspect(self.engine).get_table_names(schema=target_schema)
        check = target_table in tables
        if not check:
            self.create_sql_schema(target_table, target_schema)
        with self.engine.connect() as connection:
            if mode == "replace":
                self.truncate_table(target_table, target_schema)
            try:
                dataframe.to_sql(target_table, con=connection, if_exists = "append", schema = target_schema, index = False)
            except PendingRollbackError:
                connection.execute("ROLLBACK;")
                dataframe.to_sql(target_table, con=connection, if_exists = "append", schema = target_schema, index = False)
