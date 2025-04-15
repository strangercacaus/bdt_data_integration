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
("CONTENT"->>'caract_value_first_dimension')::integer AS caract_value_first_dimension,
("CONTENT"->>'caract_value_second_dimension')::integer AS caract_value_second_dimension,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_product_sku_data')::integer AS id_product_sku_data,
("CONTENT"->>'active')::boolean AS active
FROM {{source('bendito','bdt_raw_product_sku')}}