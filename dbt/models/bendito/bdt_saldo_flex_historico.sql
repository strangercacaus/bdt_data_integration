SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'saldo_flex_id')::integer AS saldo_flex_id,
("CONTENT"->>'invoice_id')::integer AS invoice_id,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'movement')::integer AS movement,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'observation')::character varying AS observation
FROM {{source('bendito','bdt_raw_saldo_flex_historico')}}