
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'access_token')::text AS access_token,
("CONTENT"->>'expires_in')::character varying AS expires_in,
("CONTENT"->>'token_type')::character varying AS token_type,
("CONTENT"->>'scope')::text AS scope,
("CONTENT"->>'refresh_token')::character varying AS refresh_token,
("CONTENT"->>'customer_id')::integer AS customer_id,
("CONTENT"->>'client_id')::character varying AS client_id,
("CONTENT"->>'client_secret')::character varying AS client_secret
FROM "bendito_intelligence"."bendito"."bdt_raw_token_info"