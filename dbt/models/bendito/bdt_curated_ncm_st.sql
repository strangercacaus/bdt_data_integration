{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id_ncm')::integer AS id_ncm,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'state_start')::integer AS state_start,
("CONTENT"->>'state_finish')::integer AS state_finish,
("CONTENT"->>'variation_finish')::integer AS variation_finish,
("CONTENT"->>'st')::numeric AS st,
("CONTENT"->>'difa_st')::numeric AS difa_st,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'variation_start')::integer AS variation_start,
("CONTENT"->>'mva')::numeric AS mva,
("CONTENT"->>'aliquota_interna_produto')::numeric AS aliquota_interna_produto,
("CONTENT"->>'fecp')::numeric AS fecp
FROM {{source('bendito','bdt_raw_ncm_st')}}