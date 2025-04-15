{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'price_table_id')::integer AS price_table_id,
("CONTENT"->>'payment_method_id')::integer AS payment_method_id
FROM {{source('bendito','bdt_raw_payment_method_price_table')}}