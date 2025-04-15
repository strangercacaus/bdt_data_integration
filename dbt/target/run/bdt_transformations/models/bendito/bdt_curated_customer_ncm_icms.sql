
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_customer_ncm_icms__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer_ncm')::integer AS id_customer_ncm,
("CONTENT"->>'state')::integer AS state,
("CONTENT"->>'icms_reducao_base_calculo')::numeric AS icms_reducao_base_calculo,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_customer_ncm_icms"
  );
  