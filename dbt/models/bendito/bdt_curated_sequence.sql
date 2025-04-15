{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "GRANT SELECT ON {{ this }} TO bendito_metabase",
        ],
    )}}
SELECT ("CONTENT"->>'customer')::integer AS customer,
("CONTENT"->>'object')::character varying AS object,
("CONTENT"->>'value')::integer AS value
FROM {{source('bendito','bdt_raw_sequence')}}