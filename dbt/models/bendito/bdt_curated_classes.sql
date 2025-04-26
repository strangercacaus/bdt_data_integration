SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'append_product_description')::character varying AS append_product_description,
("CONTENT"->>'product_model')::integer AS product_model
FROM {{source('bendito','bdt_raw_classes')}}