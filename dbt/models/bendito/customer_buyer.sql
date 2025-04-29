SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_buyer')::integer AS id_buyer,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'id_user')::integer AS id_user
FROM {{source('bendito','bdt_raw_customer_buyer')}}