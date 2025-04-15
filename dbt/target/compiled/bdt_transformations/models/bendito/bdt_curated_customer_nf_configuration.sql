
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'customer')::integer AS customer,
("CONTENT"->>'operation_nature')::integer AS operation_nature,
("CONTENT"->>'additional_information')::text AS additional_information,
("CONTENT"->>'additional_tax_information')::text AS additional_tax_information,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_customer_nf_configuration"