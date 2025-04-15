
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_customer_user__dbt_tmp"
  
  
    as
  
  (
    
SELECT 
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'integration_code')::text AS integration_code,
("CONTENT"->>'price_table')::integer[] AS price_table,
("CONTENT"->>'is_admin')::boolean AS is_admin,
("CONTENT"->>'status')::integer AS status
FROM "bendito_intelligence"."bendito"."bdt_raw_customer_user"
  );
  