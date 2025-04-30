SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'notes')::text AS notes,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'person')::integer AS person,
("CONTENT"->>'id_business_unit')::integer AS id_business_unit,
("CONTENT"->>'integration_code')::character varying AS integration_code
FROM {{source('bendito','bdt_raw_business_unit')}}