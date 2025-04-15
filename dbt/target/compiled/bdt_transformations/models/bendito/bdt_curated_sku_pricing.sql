
SELECT ("CONTENT"->>'id_sku')::integer AS id_sku,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'price')::numeric AS price,
("CONTENT"->>'validity')::timestamp without time zone AS validity,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'minimum_price')::numeric AS minimum_price
FROM "bendito_intelligence"."bendito"."bdt_raw_sku_pricing"