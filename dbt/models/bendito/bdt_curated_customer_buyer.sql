{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",
            "CREATE INDEX IF NOT EXISTS idx_customer_buyer_id_buyer ON {{ this }} (id_buyer)",
            "CREATE INDEX IF NOT EXISTS idx_customer_buyer_id_client ON {{ this }} (id_client)",
            "CREATE INDEX IF NOT EXISTS idx_customer_buyer_id_user ON {{ this }} (id_user)",
            "CREATE INDEX IF NOT EXISTS idx_customer_buyer_time_creation ON {{ this }} (time_creation)",
            "CREATE INDEX IF NOT EXISTS idx_customer_buyer_time_modification ON {{ this }} (time_modification)"
        ],
    )}}
SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_buyer')::integer AS id_buyer,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'id_user')::integer AS id_user
FROM {{source('bendito','bdt_raw_customer_buyer')}}