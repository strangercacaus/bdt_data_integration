
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'workflow_status')::integer AS workflow_status,
("CONTENT"->>'start_date')::timestamp without time zone AS start_date,
("CONTENT"->>'validity_date_type')::integer AS validity_date_type,
("CONTENT"->>'validity_date_value')::character varying AS validity_date_value,
("CONTENT"->>'ocurrency_quantity')::integer AS ocurrency_quantity,
("CONTENT"->>'recurrency')::character varying AS recurrency,
("CONTENT"->>'id_owner')::integer AS id_owner,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_workflow')::integer AS id_workflow,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_route"