SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'type_list')::integer AS type_list
FROM {{source('bendito','bdt_raw_user_configuration')}}