class DataTable:
    def __init__(
        self,
        id=None,
        origin: str = None,
        source_name: str = None,
        source_identifier: str = None,
        target_name: str = None,
        active: bool = True,
        unique_id_property: str = None,
        updated_at_property: str = None,
        extraction_strategy: str = None,
        materialization_strategy: str = None,
        days_interval: int = None,
        last_successful_sync_at: str = None,
        last_sync_attempt_at: str = None,
        created_at: str = None,
        updated_at: str = None,
        run_dbt_processed: bool = True,
        run_dbt_curated: bool = True,
        index_columns: list = None,
    ):
        self.id = id
        self.origin = origin
        self.source_name = source_name
        self.source_identifier = source_identifier
        self.target_name = target_name
        self.active = active
        self.unique_id_property = unique_id_property
        self.updated_at_property = updated_at_property
        self.extraction_strategy = extraction_strategy
        self.materialization_strategy = materialization_strategy
        self.days_interval = days_interval
        self.last_successful_sync_at = last_successful_sync_at
        self.last_sync_attempt_at = last_sync_attempt_at
        self.created_at = created_at
        self.updated_at = updated_at
        self.run_dbt_processed = run_dbt_processed
        self.run_dbt_curated = run_dbt_curated
        self.index_columns = index_columns

    @property
    def raw_model_name(self):
        return (
            f"{self.get_suffix(self.origin)}_raw_{self.source_name.replace('.', '_')}"
        )
    @property
    def processed_model_name(self):
        return f"{self.get_suffix(self.origin)}_processed_{self.source_name.replace('.', '_')}"
    @property
    def curated_model_name(self):
        return f"{self.get_suffix(self.origin)}_{self.source_name.replace('.', '_')}"
    @property
    def schemaless_ddl(self):
        return f"""CREATE TABLE IF NOT EXISTS {self.origin}.{self.raw_model_name}(
                    "ID" varchar NOT NULL,
                    "SUCCESS" bool,
                    "CONTENT" jsonb
                    );"""

    def get_suffix(self, origin):
        origin_suffixes = {"bendito": "bdt", "notion": "ntn", "bitrix": "btx"}
        if origin not in origin_suffixes:
            raise ValueError(f"Unsupported origin: {origin}")
        return origin_suffixes[origin]