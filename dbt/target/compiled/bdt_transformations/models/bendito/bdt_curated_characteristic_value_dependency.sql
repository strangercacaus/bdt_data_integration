
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_value')::integer AS id_value,
("CONTENT"->>'condition')::integer AS condition,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_characteristic_value_dependency"