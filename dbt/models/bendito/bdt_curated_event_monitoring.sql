{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",
            "CREATE INDEX IF NOT EXISTS idx_id_customer ON {{ this }} (id_customer)",
            "CREATE INDEX IF NOT EXISTS idx_id_user ON {{ this }} (id_user)"
        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'endpoint')::character varying AS endpoint,
("CONTENT"->>'action')::character varying AS action,
("CONTENT"->>'method')::character varying AS method,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation
FROM {{source('bendito','bdt_raw_event_monitoring')}}