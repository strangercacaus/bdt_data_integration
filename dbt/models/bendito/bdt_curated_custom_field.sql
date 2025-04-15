{{ config(
        materialized = 'table',
        unique_key = 'id',
        post_hook=[
            "GRANT SELECT ON {{ this }} TO bendito_metabase"
        ],
    )}}
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'name')::character varying AS name,
("CONTENT"->>'is_required')::boolean AS is_required,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'values')::varchar[] AS values,
("CONTENT"->>'order')::integer AS order,
("CONTENT"->>'custom_group_id')::integer AS custom_group_id,
("CONTENT"->>'integration_name')::character varying AS integration_name
FROM {{source('bendito','bdt_raw_custom_field')}}