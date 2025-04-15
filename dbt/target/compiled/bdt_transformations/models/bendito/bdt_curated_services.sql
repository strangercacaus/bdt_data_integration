
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'code')::character varying AS code,
("CONTENT"->>'description')::character varying AS description
FROM "bendito_intelligence"."bendito"."bdt_raw_services"