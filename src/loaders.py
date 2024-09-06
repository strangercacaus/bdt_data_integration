class DataIngestion:
    def __init__(self, staging_dir, db_url):
        self.staging_dir = staging_dir
        self.engine = create_engine(db_url)
        self.metadata = MetaData()

    def _get_csv_files(self):
        return [f for f in os.listdir(self.staging_dir) if f.endswith('.csv')]

    def _infer_table_schema(self, df):
        return pd.io.sql.get_schema(df, 'temp_table', con=self.engine)

    def _create_table(self, table_name, df):
        df.head(0).to_sql(table_name, self.engine, if_exists='replace', index=False)

    def _upsert(self, table, conn, keys, data_iter):
        insert_stmt = insert(table.table).values(list(data_iter))
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=keys,
            set_={c.key: c for c in insert_stmt.excluded if c.key not in keys}
        )
        conn.execute(update_stmt)

    def ingest_data(self, table_name, primary_key=None, mode='append'):
        csv_files = self._get_csv_files()
        for file in csv_files:
            file_path = os.path.join(self.staging_dir, file)
            df = pd.read_csv(file_path)
            
            if not self.engine.dialect.has_table(self.engine, table_name):
                self._create_table(table_name, df)
            
            with self.engine.connect() as conn:
                table = Table(table_name, self.metadata, autoload_with=self.engine)
                if mode == 'upsert' and primary_key:
                    self._upsert(table, conn, primary_key, df.to_dict(orient='records'))
                else:
                    df.to_sql(table_name, self.engine, if_exists='append', index=False)