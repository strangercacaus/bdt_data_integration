SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_team')::integer AS id_team,
("CONTENT"->>'id_reference')::integer AS id_reference,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_team_child')}}