{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'caract_first_dimension')::integer AS caract_first_dimension,
("CONTENT"->>'caract_second_dimension')::integer AS caract_second_dimension,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_matrix')}}