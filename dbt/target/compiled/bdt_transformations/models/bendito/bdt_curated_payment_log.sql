
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'customer_id')::integer AS customer_id,
("CONTENT"->>'endpoint')::character varying AS endpoint,
("CONTENT"->>'request')::character varying AS request,
("CONTENT"->>'response')::character varying AS response
FROM "bendito_intelligence"."bendito"."bdt_raw_payment_log"