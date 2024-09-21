import os
import sys
import csv
import logging
import psycopg2
import warnings

import pandas as pd
from jinja2 import Template
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError, ProgrammingError, ObjectNotExecutableError

sys.path.append(os.path.abspath('bdt_data_integration'))
from src.utils import Utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)




class PostgresLoader:
    """
    Classe para carregar dados em um banco de dados PostgreSQL.

    Esta classe fornece métodos para criar tabelas, gerenciar esquemas e carregar dados de dfs no PostgreSQL. 
    Também suporta a criação de gatilhos e restrições para manter a integridade dos dados.

    Atributos:
        user (str): O nome de usuário para a conexão com o banco de dados PostgreSQL.
        password (str): A senha para a conexão com o banco de dados PostgreSQL.
        host (str): O endereço do host do banco de dados PostgreSQL.
        db_name (str): O nome do banco de dados PostgreSQL.
        jdbc_string (str): A string de conexão JDBC para o banco de dados PostgreSQL.
        engine: O engine do SQLAlchemy para conectar ao banco de dados PostgreSQL.
    """
    def __init__(self, user: str, password: str, host: str, db_name: str, schema_file_path:str = None, schema_file_type = list ['template','info_schema']):
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
        self.jdbc_string = (f"postgresql://{user}:{password}@{host}/{db_name}?sslmode=require")
        self.engine = create_engine(self.jdbc_string)
        self.schema_file_path = schema_file_path
        self.schema_file_type = schema_file_type

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
        columns = []
        # Step 1: Read the CSV file
        with open(self.schema_file_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter = ';')
            for row in reader:
                # Step 2: Filter by table name
                if row['table_name'] == target_table:
                    column_def = f"{row['column_name']} {row['udt_name']}"
                    if row['character_maximum_length']:
                        column_def += f"({int(float(row['character_maximum_length']))})"
                    #if row['is_nullable'] == 'NO':
                    #    column_def += ' NOT NULL'
                    columns.append(column_def)
        if columns:
            columns_definition = ',\n    '.join(columns)
            return f"CREATE TABLE {target_schema}.{target_table} (\n    {columns_definition}\n);"
        else:
            raise ValueError(f"No columns found for table '{target_table}'")
            
    
    def render_sql_template(self, target_table: str, target_schema: str):
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
        context = {
            "target_table": target_table,
            "target_schema": target_schema,
        }
        try:
            with open(self.schema_file_path, "r", encoding="utf-8") as file:
                text = file.read()
                template = Template(text)
        except:
            raise Exception('Problema ao abrir o arquivo de schema.')
        return template.render(context)

        # Step 3: Generate the CREATE TABLE query
        if columns:
            columns_definition = ',\n    '.join(columns)
            return f"CREATE TABLE {target_schema}.{target_table} (\n    {columns_definition}\n);"
        else:
            raise ValueError(f"No columns found for table '{target_table}'")

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
            try:
                connection.autocommit = True  # To allow session termination
                create_query = f"CREATE TRIGGER set_updated_at_{target_table} BEFORE UPDATE ON {target_schema}.{target_table} FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();"
                connection.execute(create_query)
            except ProgrammingError as e:
                if isinstance(e.orig, psycopg2.errors.DuplicateObject):
                    logger.warning(f"O trigger set_updated_at_{target_table} já existe, pulando a criação.")
                else:
                    raise e
            except Exception as e:
                raise e

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
        if self.schema_file_type == 'info_schema':
            sql_command = self.load_sql_schema(target_table, target_schema)
        elif self.schema_file_type == 'template':
            sql_command = self.render_sql_template(target_table, target_schema)
        else:
            raise ValueError("É obrigatório escolher entre um de 'info_schema' ou 'template'")
        if Utils.validate_sql(sql_command) == False:
            logger.error(f'SQL Inválido em {__name__}: {sql_command}')
        with self.engine.connect() as connection:
            try:
                connection.execute(sql_command)
            except SQLAlchemyError as e:
                if isinstance(e, ObjectNotExecutableError):
                    logger.error(f"O comando SQL não é executável: {sql_command}")
                elif isinstance(e.orig, psycopg2.errors.DuplicateTable):
                    logger.warning(f"Table {target_table} already exists.")
                else:
                    logger.error(f"An error occurred: {e}")

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
        logger.info('loader.create_sql_schema, target_table: ' + target_table + ', target_schema: ' + target_schema)

        self.create_table(target_table, target_schema)

        if primary_key != None:
            self.create_constraint(
                target_table, target_schema, column, kind="primary_key"
            )
        if unique_columns != None:
            for column in unique_columns:
                self.create_constraint(
                    target_table, target_schema, column, kind="unique_key"
                )

        # ifself.create_update_updated_at_function()
        # logger.info(f'Função update_updated_at {target_table} criada.')
        # self.create_updated_at_trigger(target_table, target_schema)
    
    def check_if_schema_exists(self, target_schema):
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            check_schema_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{target_schema}';"
            result = connection.execute(check_schema_query)
            
            # Fetch the first result
            schema_exists = result.fetchone() is not None
            
            if schema_exists:
                logger.info(f'Schema {target_schema} encontrado.')
            else:
                logger.info(f'Schema {target_schema} não existe.')
            
            return schema_exists

    
    def create_schema(self, target_schema):
        with self.engine.connect() as connection:
            connection.autocommit = True  # To allow session termination
            create_schema_query = f"CREATE SCHEMA {target_schema};"
            result = connection.execute(create_schema_query)
            logger.info(f'Schema {target_schema} criado com sucesso.')

    def load_data(
        self,
        df: pd.DataFrame,
        target_table: str,
        target_schema: str,
        mode=("append", "replace"),
        **kwargs):
        """
        Carrega os dados de um df na tabela de destino.

        Este método verifica se a tabela existe e, se não existir, cria o esquema SQL. 
        Em seguida, carrega os dados do df na tabela, podendo substituir os dados existentes.

        Args:
            df (pd.DataFrame): O df contendo os dados a serem carregados.
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
            mode (tuple, optional): O modo de carregamento ('append' para adicionar, 'replace' para substituir).
        """
        schema_exists = self.check_if_schema_exists(target_schema)
        if not schema_exists:
            self.create_schema(target_schema)

        self.close_connections()

        tables = inspect(self.engine).get_table_names(schema=target_schema)
        chunksize = kwargs.get('chunksize',1000)
        check = target_table in tables
        if not check:
            if self.schema_file_path == None:
                raise Exception('Relação não existe no destino, caminho do arquivo de schema precisa estar presente.')
            logger.info(f'Criando tabela {target_table} em {target_schema}')
            self.create_sql_schema(target_table, target_schema)
        with self.engine.connect() as connection:
            loaded_rows = 0
            if mode == "replace":
                logger.info(f'Truncando dados de {target_table}.')
                self.truncate_table(target_table, target_schema)
            try:
                logger.info(f'Inserindo dados em {target_table}.')
                loaded_rows = df.to_sql(target_table, con=connection, if_exists = "append", schema = target_schema, index = False, chunksize = chunksize)
            except PendingRollbackError:
                logger.info(f'Rollback pendente detectado, realizando operação.')
                connection.execute("ROLLBACK;")
                logger.info(f'Inserindo dados em {target_table}.')
                loaded_rows = df.to_sql(target_table, con=connection, if_exists = "append", schema = target_schema, index = False, chunksize = chunksize)
        logger.info(f'Fim do carregamento de dados em {target_table}, {loaded_rows} linhas inseridas.')