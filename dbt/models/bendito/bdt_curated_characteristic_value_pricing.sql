SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_value')::integer AS id_value,
("CONTENT"->>'condition')::integer AS condition,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'multiplier')::json AS multiplier,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_characteristic_value_pricing')}}