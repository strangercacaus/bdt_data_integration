{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'name')::character varying AS name,
("CONTENT"->>'entity')::integer AS entity,
("CONTENT"->>'order')::integer AS order,
("CONTENT"->>'customer_id')::integer AS customer_id
FROM {{source('bendito','bdt_raw_custom_group')}}