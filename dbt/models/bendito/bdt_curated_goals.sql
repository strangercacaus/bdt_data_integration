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
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'seller_ids')::integer[] AS seller_ids,
("CONTENT"->>'team_ids')::integer[] AS team_ids,
("CONTENT"->>'initial_value')::numeric AS initial_value,
("CONTENT"->>'goal_value')::numeric AS goal_value,
("CONTENT"->>'type_period')::integer AS type_period,
("CONTENT"->>'status_period')::integer[] AS status_period,
("CONTENT"->>'start_date')::timestamp without time zone AS start_date,
("CONTENT"->>'end_date')::timestamp without time zone AS end_date,
("CONTENT"->>'type_goal')::integer AS type_goal,
("CONTENT"->>'type_measure_goal')::integer AS type_measure_goal,
("CONTENT"->>'unit_measure')::integer AS unit_measure,
("CONTENT"->>'value_with_tax')::boolean AS value_with_tax,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_goals')}}