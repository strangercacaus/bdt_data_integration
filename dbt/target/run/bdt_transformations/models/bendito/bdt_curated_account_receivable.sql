
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_account_receivable__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'client_integration_code')::character varying AS client_integration_code,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'date_of_issue')::date AS date_of_issue,
("CONTENT"->>'forecast_date')::date AS forecast_date,
("CONTENT"->>'due_date')::date AS due_date,
("CONTENT"->>'doc')::character varying AS doc,
("CONTENT"->>'status')::character varying AS status,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'link_boleto')::text AS link_boleto,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_account_receivable"
  );
  