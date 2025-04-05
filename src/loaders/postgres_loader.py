import csv
import logging
import psycopg2
from typing import Literal

import pandas as pd
from jinja2 import Template
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import (
    SQLAlchemyError,
    PendingRollbackError,
    ProgrammingError,
    ObjectNotExecutableError,
)

from utils import Utils
from loaders.base_loader import BaseLoader

# Create a named logger for this module
logger = logging.getLogger("postgres_loader")


class PostgresLoader(BaseLoader):
    """
    Classe para carregar dados em um banco de dados PostgreSQL.

    Esta classe fornece métodos para criar tabelas, gerenciar esquemas e carregar dados de DataFrames no PostgreSQL.
    Também suporta a criação de gatilhos e restrições para manter a integridade dos dados.

    Atributos:
        engine (Engine): O engine do SQLAlchemy para conectar ao banco de dados PostgreSQL.
        schema_file_path (str): Caminho para o arquivo de esquema de tabela.
        schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema.
    """

    def __init__(
        self,
        engine: Engine = None,
        schema_file_path: str = None,
        schema_file_type: Literal["template", "info_schema", "schema"] = "template",
    ):
        """
        Inicializa o PostgresLoader com os parâmetros de conexão do banco de dados.

        Args:
            engine (Engine): O engine do SQLAlchemy para conectar ao banco de dados PostgreSQL.
            schema_file_path (str): Caminho para o arquivo de esquema de tabela.
            schema_file_type (Literal["template", "info_schema", "schema"]): Tipo de arquivo de esquema.
        """
        self.engine = engine
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
        with open(self.schema_file_path, mode="r") as csvfile:

            reader = csv.DictReader(csvfile, delimiter=";")

            for row in reader:
                # Step 2: Filter by table name
                if row["table_name"] == target_table:
                    column_def = f""""{row['column_name']}" {row['udt_name']}"""
                    if row["character_maximum_length"]:
                        column_def += f"({int(float(row['character_maximum_length']))})"
                    columns.append(column_def)
        if columns:

            columns_definition = ",\n    ".join(columns)
            return f"CREATE TABLE {target_schema}.{target_table} (\n    {columns_definition}\n);"

        else:
            raise ValueError(
                f"Nenhuma coluna encontrada para a tabela '{target_table}'"
            )

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
            raise Exception("Problema ao abrir o arquivo de schema.")

        return template.render(context)

    def close_connections(self):
        """
        Fecha as conexões ativas com o banco de dados.

        Este método termina todas as conexões ativas para o banco de dados especificado.
        """
        with self.engine.connect() as connection:
            db_name = self.engine.url.database
            terminate_query = text(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity JOIN pg_roles ON pg_stat_activity.usename = pg_roles.rolname WHERE pid <> pg_backend_pid() AND datname = '{db_name}' AND NOT pg_roles.rolsuper;"
            )
            connection.execute(terminate_query)

    def create_schema(self, target_schema):
        """
        Cria um novo schema no banco de dados.

        Este método verifica se o schema já existe e, caso não exista, cria um novo schema
        com o nome especificado.

        Args:
            target_schema (str): O nome do schema a ser criado.
        """
        with self.engine.begin() as connection:
            create_schema_query = text(f"CREATE SCHEMA {target_schema}")
            logger.debug(f'Executando query: {create_schema_query}')
            connection.execute(create_schema_query)
            logger.info(f"Schema {target_schema} criado com sucesso.")

    def create_table(self, sql_command):
        """
        Cria a tabela de destino com base no esquema SQL carregado.

        Este método executa o comando SQL para criar a tabela de destino e lida com erros
        caso a tabela já exista.

        Args:
            sql_command (str): O comando SQL para criar a tabela.
        """
        if Utils.validate_sql(sql_command) == False:
            raise ValueError(f"SQL Inválido em {__name__}: {sql_command}")

        with self.engine.begin() as connection:
            try:
                connection.execute(text(sql_command))
            except SQLAlchemyError as e:
                if isinstance(e, ObjectNotExecutableError):
                    logger.error(f"O comando SQL não é executável: {sql_command}")
                elif isinstance(e.orig, psycopg2.errors.DuplicateTable):
                    logger.error(f"{__name__}: A tabela já existe.")
                else:
                    logger.error(f"Um erro ocorreu: {e}")
                raise e

    def truncate_table(self, target_table: str, target_schema: str):
        """
        Trunca a tabela de destino, removendo todos os registros.

        Este método remove todos os registros da tabela especificada, mantendo a estrutura da tabela.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
        """
        with self.engine.begin() as connection:
            truncate_query = text(f"TRUNCATE TABLE {target_schema}.{target_table}")
            connection.execute(truncate_query)

    def drop_table(self, target_table: str, target_schema: str):
        """
        Deleta a tabela de destino.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
        """
        with self.engine.begin() as connection:
            drop_query = text(f"DROP TABLE {target_schema}.{target_table}")
            connection.execute(drop_query)

    def create_updated_at_trigger(self, target_table, target_schema):
        """
        Cria um gatilho para atualizar a coluna de data de modificação.

        Este método cria um gatilho que atualiza a coluna de data de modificação
        sempre que uma linha na tabela é atualizada.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
        """
        with self.engine.begin() as connection:
            try:
                create_query = text(
                    f"CREATE TRIGGER set_updated_at_{target_table} BEFORE UPDATE ON {target_schema}.{target_table} FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()"
                )
                connection.execute(create_query)
            except ProgrammingError as e:
                if isinstance(e.orig, psycopg2.errors.DuplicateObject):
                    logger.warning(
                        f"O trigger set_updated_at_{target_table} já existe, pulando a criação."
                    )
                else:
                    raise e
            except Exception as e:
                raise e

    def create_update_updated_at_function(self):
        """
        Cria uma função para atualizar a coluna de data de modificação.

        Este método cria ou substitui uma função que define a coluna de data de modificação
        para o timestamp atual.
        """
        with self.engine.begin() as connection:
            create_query = text(
                "CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.__updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ LANGUAGE plpgsql"
            )
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
        with self.engine.begin() as connection:
            if kind == "primary_key":
                create_query = text(
                    f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}_pk PRIMARY KEY ("{column}")"""
                )
            elif kind == "unique_key":
                create_query = text(
                    f"""ALTER TABLE {target_schema}.{target_table} ADD CONSTRAINT {target_table}__unique UNIQUE ("{column}")"""
                )
            connection.execute(create_query)

    def create_sql_schema(self, target_table: str, target_schema: str):
        """
        Cria o esquema SQL para a tabela de destino.

        Este método cria a tabela e, se especificado, adiciona restrições de chave primária
        e colunas únicas, além de configurar a função e o gatilho para a coluna de data de modificação.

        Args:
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela será criada.
            **kwargs: Argumentos adicionais:
                - schema (str): Definição SQL da tabela quando schema_file_type='schema'.
        """
        logger.info(
            f"loader.create_sql_schema, target_table: {target_table}, target_schema: {target_schema}, type: {self.schema_file_type}"
        )

        if self.schema:
            
            sql_command = self.schema
            
        elif self.schema_file_type == "info_schema":

            sql_command = self.load_sql_schema(target_table, target_schema)

        elif self.schema_file_type == "template":

            sql_command = self.render_sql_template(target_table, target_schema)

        else:
            raise ValueError(
                "É obrigatório escolher entre um de 'info_schema', 'template' ou 'schema'"
            )

        self.create_table(sql_command)

    def check_if_schema_exists(self, target_schema):
        """
        Verifica se um schema existe no banco de dados.

        Este método consulta o catálogo do sistema para verificar se um schema
        com o nome especificado já existe no banco de dados.

        Args:
            target_schema (str): O nome do schema a ser verificado.

        Returns:
            bool: True se o schema existir, False caso contrário.
        """
        with self.engine.connect() as connection:
            # Consultas de leitura não precisam de transação explícita
            check_schema_query = text(
                f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{target_schema}'"
            )
            result = connection.execute(check_schema_query)

            # Fetch the first result
            schema_exists = result.fetchone() is not None

            if schema_exists:
                logger.info(f"Schema {target_schema} encontrado.")
            else:
                logger.info(f"Schema {target_schema} não existe.")

            return schema_exists

    def load_data(
        self,
        df: pd.DataFrame,
        target_table: str,
        target_schema: str,
        mode="replace",
        **kwargs,
    ):
        """
        Carrega os dados de um DataFrame na tabela de destino.

        Este método verifica se a tabela existe e, se não existir, cria o esquema SQL.
        Em seguida, carrega os dados do DataFrame na tabela, podendo substituir os dados existentes.

        Args:
            df (pd.DataFrame): O DataFrame contendo os dados a serem carregados.
            target_table (str): O nome da tabela de destino.
            target_schema (str): O esquema de destino onde a tabela está localizada.
            mode (str): O modo de carregamento ('append' para adicionar, 'replace' para substituir).
            **kwargs: Argumentos adicionais:
                - chunksize (int): Tamanho dos blocos para carregamento em lotes.
                - table_definition (str): Definição SQL da tabela quando schema_file_type='schema'.
        """
        logger.debug(
            f"Iniciando load_data para {target_table} em {target_schema}, modo: {mode}"
        )

        # Define o tamanho do chunk para o carregamento em partes
        chunksize = kwargs.get("chunksize", 1000)
        # Define o schema para o carregamento
        table_definition = kwargs.get("schema", None)
        # Verifica se o schema existe
        schema_exists = self.check_if_schema_exists(target_schema)

        # Se o schema não existe, cria o schema
        if schema_exists == False:
            logger.debug(f"Schema {target_schema} não existe, criando agora")
            self.create_schema(target_schema)

        # Verifica se a tabela existe no schema
        tables = inspect(self.engine).get_table_names(schema=target_schema)

        # Se a tabela existe no schema, carrega os dados
        if target_table in tables:
            logger.debug(f"Tabela {target_table} encontrada em {target_schema}")

            # Se o modo é replace, trunca a tabela para limpar os dados antes do insert
            if mode == "replace":
                logger.debug(f"Truncando dados de {target_table}.")
                self.truncate_table(target_table, target_schema)

                # Verifica se o engine é uma instância de um Sqlalchemy Engine
                if not isinstance(self.engine, Engine):
                    raise ValueError(
                        "O engine não é uma instância de um Sqlalchemy Engine."
                    )
                else:
                    logger.debug(f"Validação do engine passou: {type(self.engine)}")
        # Se a tabela não existe no schema, cria a tabela
        else:
            logger.debug(f"Tabela {target_table} não encontrada em {target_schema}")

            if table_definition is None and self.schema_file_path is None:
                raise ValueError(
                    "Relação não existe no destino, caminho do arquivo de schema precisa estar presente."
                )

            logger.debug(f"Criando tabela {target_table} em {target_schema}")
            self.create_sql_schema(target_table, target_schema)

        loaded_rows = 0
        logger.debug(f"Inserindo dados em {target_table}")
        
        loaded_rows = df.to_sql(
            target_table,
            con=self.engine,
            if_exists="append",
            schema=target_schema,
            index=False,
            chunksize=chunksize,
        )

        logger.debug(
            f"Fim do carregamento de dados em {target_table}, {loaded_rows} linhas inseridas."
        )
