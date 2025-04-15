{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'enabled')::boolean AS enabled,
("CONTENT"->>'discount_limit')::numeric AS discount_limit,
("CONTENT"->>'addition_limit')::numeric AS addition_limit,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'id_customer')::integer AS id_customer
FROM {{source('bendito','bdt_raw_saldo_flex')}}