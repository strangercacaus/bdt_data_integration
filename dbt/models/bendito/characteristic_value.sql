SELECT ("CONTENT"->>'id_characteristic')::integer AS id_characteristic,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_value')::integer AS id_value,
("CONTENT"->>'append_product_description')::character varying AS append_product_description
FROM {{source('bendito','bdt_raw_characteristic_value')}}