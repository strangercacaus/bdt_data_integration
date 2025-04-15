
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_warehouse__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'sale')::boolean AS sale,
("CONTENT"->>'default')::boolean AS default
FROM "bendito_intelligence"."bendito"."bdt_raw_warehouse"
  );
  