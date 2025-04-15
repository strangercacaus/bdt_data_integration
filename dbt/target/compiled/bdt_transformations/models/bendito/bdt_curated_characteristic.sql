
SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'unit_measure')::integer AS unit_measure,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_characteristic')::integer AS id_characteristic,
("CONTENT"->>'id_open_values')::boolean AS id_open_values,
("CONTENT"->>'priority')::integer AS priority
FROM "bendito_intelligence"."bendito"."bdt_raw_characteristic"