SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'question')::integer AS question,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'recurrence')::integer AS recurrence,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_feedback_question')}}