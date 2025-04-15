{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'validity')::timestamp without time zone AS validity,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'minimum_price')::numeric AS minimum_price,
("CONTENT"->>'type_calc')::integer AS type_calc,
("CONTENT"->>'minimum_percentage')::numeric AS minimum_percentage,
("CONTENT"->>'id_family')::integer[] AS id_family
FROM {{source('bendito','bdt_raw_price_table')}}