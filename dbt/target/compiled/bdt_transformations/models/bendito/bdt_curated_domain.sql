
SELECT ("CONTENT"->>'domain')::character varying AS domain,
("CONTENT"->>'value')::integer AS value,
("CONTENT"->>'language')::character varying AS language,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'short_description')::character varying AS short_description,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation
FROM "bendito_intelligence"."bendito"."bdt_raw_domain"