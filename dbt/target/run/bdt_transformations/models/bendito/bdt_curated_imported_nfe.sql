
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_imported_nfe__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'client_integration_code')::character varying AS client_integration_code,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'invoice_integration_code')::character varying AS invoice_integration_code,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'access_key')::character varying AS access_key,
("CONTENT"->>'number')::character varying AS number,
("CONTENT"->>'date_of_issue')::date AS date_of_issue,
("CONTENT"->>'date_of_registry')::date AS date_of_registry,
("CONTENT"->>'link_danfe')::text AS link_danfe,
("CONTENT"->>'files')::text[] AS files,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_imported_nfe"
  );
  