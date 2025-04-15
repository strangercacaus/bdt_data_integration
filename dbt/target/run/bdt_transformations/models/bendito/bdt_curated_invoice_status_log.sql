
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_invoice_status_log__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'status_origin')::integer AS status_origin,
("CONTENT"->>'status_destiny')::integer AS status_destiny,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'origem')::character varying AS origem
FROM "bendito_intelligence"."bendito"."bdt_raw_invoice_status_log"
  );
  