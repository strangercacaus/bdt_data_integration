SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'detailed_description')::text AS detailed_description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_mail')::integer AS id_mail,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'send_to_emails')::varchar AS send_to_emails
FROM {{source('bendito','bdt_raw_workflow')}}