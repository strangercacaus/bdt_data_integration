SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_route')::integer AS id_route,
("CONTENT"->>'id_workflow')::integer AS id_workflow,
("CONTENT"->>'body_old')::json AS body_old,
("CONTENT"->>'body_new')::json AS body_new,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'level')::integer AS level,
("CONTENT"->>'approved')::boolean AS approved,
("CONTENT"->>'finished')::boolean AS finished,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_route_workflow')}}