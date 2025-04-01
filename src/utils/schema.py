import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Schema:
    """
    Schema class for handling data schema definitions.
    
    Schema precisa ser um dataframe com 3 colunas: 'column_name', 'source_type', 'destination_type'
    """
    def __init__(self, schema_df: pd.DataFrame):
        self.schema_df = schema_df

    def render_ddl(self, target_schema, target_table):
        """
        Renders the DDL (Data Definition Language) for creating a table.
        
        Args:
            target_schema (str): The target schema name
            target_table (str): The target table name
            
        Returns:
            str: The SQL DDL statement for creating the table
        """
        columns = []
        for i in range(len(self.schema_df)):
            row = self.schema_df.iloc[i]
            column_def = f""""{row.iloc[0]}" {row.iloc[2]}"""
            columns.append(column_def)    
        
        if columns:
            columns_definition = ',\n    '.join(columns)
            return f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (\n    {columns_definition}\n);"
        else:
            raise ValueError(f"No columns found for table '{target_table}'")
    
    def render_result_df(self):
        """
        Creates an empty DataFrame with the schema's column names.
        
        Returns:
            pd.DataFrame: An empty DataFrame with the schema's column names
        """
        columns = self.schema_df['column_name'].tolist()
        return pd.DataFrame(columns=columns) 