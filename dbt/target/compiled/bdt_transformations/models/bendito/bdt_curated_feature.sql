
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'classname')::character varying AS classname,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'program')::character varying AS program,
("CONTENT"->>'parent_id')::integer AS parent_id,
("CONTENT"->>'sort')::integer AS sort,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_feature"