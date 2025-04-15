{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer_ncm')::integer AS id_customer_ncm,
("CONTENT"->>'state_start')::integer AS state_start,
("CONTENT"->>'state_finish')::integer AS state_finish,
("CONTENT"->>'variation_start')::integer AS variation_start,
("CONTENT"->>'variation_finish')::integer AS variation_finish,
("CONTENT"->>'st')::numeric AS st,
("CONTENT"->>'difa_st')::numeric AS difa_st,
("CONTENT"->>'mva')::numeric AS mva,
("CONTENT"->>'aliquota_interna_produto')::numeric AS aliquota_interna_produto,
("CONTENT"->>'fecp')::numeric AS fecp,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_customer_ncm_st')}}