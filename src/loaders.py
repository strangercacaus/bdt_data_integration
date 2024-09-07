import warnings
import logging

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, inspect, Column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Integer, Float, Boolean, DateTime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def map_dtype_to_sqlalchemy(dtype):
    if pd.api.types.is_string_dtype(dtype):
        return String
    elif pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        raise ValueError(f"Unsupported data type: {dtype}")

class DataFrameToPostgres():
    def __init__(self, jdbc_string, schema_changes='ignore'):
        """
        Inicializa a classe com a string de conexão JDBC e configurações de alteração de esquema.
        
        Args:
            jdbc_string (str): String de conexão para o banco de dados PostgreSQL.
            schema_changes (str): Como lidar com alterações de esquema (propagate, ignore, error).
        """
        self.engine = create_engine(jdbc_string)
        self.connection = self.engine.connect()
        self.metadata = MetaData(bind=self.engine)
        self.schema_changes = schema_changes
    
    def _get_table(self, table_name):
        """
        Obtém uma tabela do banco de dados.
        
        Args:
            table_name (str): Nome da tabela a ser recuperada.
        
        Returns:
            Table: Objeto da tabela SQLAlchemy.
        """
        return Table(table_name, self.metadata, autoload_with=self.engine)
    
    def _detect_schema(self, df, table_name) -> None:
        """
        Detecta e ajusta o esquema do banco de dados com base no DataFrame fornecido.
        
        Args:
            df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
            table_name (str): Nome da tabela para inserir os dados.
        """
        table = self._get_table(table_name)
        db_columns = {col.name: col.type for col in table.columns}
        logger.info(f'Colunas encontrada no Banco de Dados: {db_columns}')
        df_columns = {col: map_dtype_to_sqlalchemy(df[col].dtype) for col in df.columns}
        logger.info(f'Colunas encontrada no Dataframe: {df_columns}')

        new_columns = [col for col in df_columns if col not in db_columns]
        mismatched_columns = [col for col in df_columns if col in db_columns and str(db_columns[col]) != str(df_columns[col])]
        
        if self.schema_changes == 'propagate':
            for col in new_columns:
                table.append_column(Column(col, df_columns[col]))
            for col in mismatched_columns:
                table.columns[col].type = df_columns[col]
            self.metadata.create_all(self.engine)
        elif self.schema_changes == 'ignore':
            if new_columns or mismatched_columns:
                logger.warning(f'Schema change detected but ignored. New columns: {new_columns}, Mismatched columns: {mismatched_columns}')
            df = df[[col for col in df.columns if col in db_columns]]
        elif self.schema_changes == 'error':
            if new_columns or mismatched_columns:
                raise ValueError(f'Schema change detected. New columns: {new_columns}, Mismatched columns: {mismatched_columns}')

    def insert_data(self, df, table_name, mode: str ='append', primary_key: str = None, control_column:str = None) -> None:
        """
        Insere dados no banco de dados com base no modo especificado.
        
        Args:
            df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
            table_name (str): Nome da tabela para inserir os dados.
            mode (str): Modo de inserção ('replace', 'update', 'change-data', 'append', 'reset').
            primary_key (str): Chave primária para os modos 'update' e 'change-data'.
            control_column (str): Coluna de controle para o modo 'change-data'.
        """
        if mode not in ['replace', 'update', 'change-data', 'append', 'reset']:
            raise ValueError("Invalid mode. Choose from 'replace', 'update', 'change-data', 'append', 'reset'.")
            
        if mode == 'replace':
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        elif mode == 'append':
            self._detect_schema(df, table_name)
            df.to_sql(table_name, self.engine, if_exists='append', index=False)

        elif mode == 'reset':
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        elif mode in ['update', 'change-data']:
            if primary_key is None:
                raise ValueError("Primary key must be specified for update and change-data modes.")

            self._detect_schema(df, table_name)
            table = self._get_table(table_name)
            if mode == 'update':
                stmt = insert(table).values(df.to_dict(orient='records'))
                stmt = stmt.on_conflict_do_update(
                    index_elements=[primary_key],
                    set_={col: getattr(stmt.excluded, col) for col in df.columns if col != primary_key}
                )
            elif mode == 'change-data':
                if control_column is None:
                    raise ValueError("Control column must be specified for change-data mode.")

                stmt = insert(table).values(df.to_dict(orient='records'))
                stmt = stmt.on_conflict_do_update(
                    index_elements=[primary_key],
                    set_={col: getattr(stmt.excluded, col) for col in df.columns if col != primary_key and col != control_column},
                    where=(getattr(table.c, control_column) < getattr(stmt.excluded, control_column))
                )

            self.connection.execute(stmt)
      
    def close(self):
        """
        Fecha a conexão com o banco de dados.
        """
        self.connection.close()
        self.engine.dispose()