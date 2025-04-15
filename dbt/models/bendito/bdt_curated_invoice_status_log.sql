{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'status_origin')::integer AS status_origin,
("CONTENT"->>'status_destiny')::integer AS status_destiny,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'origem')::character varying AS origem
FROM {{source('bendito','bdt_raw_invoice_status_log')}}