SELECT ("CONTENT"->>'id_product')::integer AS id_product,
("CONTENT"->>'id_collection')::integer AS id_collection,
("CONTENT"->>'id_variation')::integer AS id_variation,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation
FROM {{source('bendito','bdt_raw_product_collection')}}