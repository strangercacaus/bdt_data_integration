
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_politic')::integer AS id_politic,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'type_value')::integer AS type_value,
("CONTENT"->>'start_value')::numeric AS start_value,
("CONTENT"->>'finish_value')::numeric AS finish_value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'values')::_varchar AS values
FROM "bendito_intelligence"."bendito"."bdt_raw_comercial_politics_rules"