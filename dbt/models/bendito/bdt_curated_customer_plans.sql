{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "GRANT SELECT ON {{ this }} TO bendito_metabase"
        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_plan')::integer AS id_plan,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'status')::integer AS status
FROM {{source('bendito','bdt_raw_customer_plans')}}