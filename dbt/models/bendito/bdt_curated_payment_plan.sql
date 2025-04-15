{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "GRANT SELECT ON {{ this }} TO bendito_metabase",

        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'due_date')::date AS due_date,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_payment_method')::integer AS id_payment_method,
("CONTENT"->>'id_meio_pagamento')::integer AS id_meio_pagamento
FROM {{source('bendito','bdt_raw_payment_plan')}}