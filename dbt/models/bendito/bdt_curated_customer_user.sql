{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",
            "CREATE INDEX IF NOT EXISTS idx_id ON {{ this }} (id_user)",
            "CREATE INDEX IF NOT EXISTS idx_id_customer ON {{ this }} (id_customer)",
            "CREATE INDEX IF NOT EXISTS idx_id_user ON {{ this }} (id_user)",
            "CREATE INDEX IF NOT EXISTS idx_time_creation ON {{ this }} (time_creation)"
        ],
    )}}
SELECT 
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'integration_code')::text AS integration_code,
("CONTENT"->>'price_table')::integer[] AS price_table,
("CONTENT"->>'is_admin')::boolean AS is_admin,
("CONTENT"->>'status')::integer AS status
FROM {{source('bendito','bdt_raw_customer_user')}}