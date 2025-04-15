
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_mail_model')::integer AS id_mail_model,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'mail')::text AS mail,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_customer')::integer AS id_customer
FROM "bendito_intelligence"."bendito"."bdt_raw_mail_model"