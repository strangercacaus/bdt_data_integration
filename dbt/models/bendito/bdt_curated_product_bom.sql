{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_product')::integer AS id_product,
("CONTENT"->>'id_item')::integer AS id_item,
("CONTENT"->>'id_component')::integer AS id_component,
("CONTENT"->>'quantity')::numeric AS quantity,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'description')::text AS description,
("CONTENT"->>'integration_code')::text AS integration_code
FROM {{source('bendito','bdt_raw_product_bom')}}