SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'state_start')::integer AS state_start,
("CONTENT"->>'state_finish')::integer AS state_finish,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_icms')}}