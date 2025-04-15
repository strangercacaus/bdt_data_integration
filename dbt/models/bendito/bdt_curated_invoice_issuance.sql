{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'code')::integer AS code,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'invoice_settings')::text AS invoice_settings,
("CONTENT"->>'invoice_issuance_return')::text AS invoice_issuance_return,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_customer_reference')::integer AS id_customer_reference
FROM {{source('bendito','bdt_raw_invoice_issuance')}}