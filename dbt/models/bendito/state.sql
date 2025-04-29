SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'country')::integer AS country,
("CONTENT"->>'ibge_code')::character varying AS ibge_code,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'short_description')::character varying AS short_description,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_state')}}