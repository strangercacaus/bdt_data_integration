SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_workflow')::integer AS id_workflow,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'level')::integer AS level,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_approver')}}