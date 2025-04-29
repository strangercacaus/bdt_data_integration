SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_pricing')::integer AS id_pricing,
("CONTENT"->>'id_char_condition')::integer AS id_char_condition,
("CONTENT"->>'signal')::integer AS signal,
("CONTENT"->>'values')::json AS values,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_characteristic_value_pricing_rules')}}