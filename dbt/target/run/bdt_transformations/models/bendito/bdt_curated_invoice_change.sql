
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_invoice_change__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'type')::character varying AS type,
("CONTENT"->>'nature')::integer AS nature,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'delivery_forecast')::timestamp without time zone AS delivery_forecast,
("CONTENT"->>'deadline')::integer AS deadline,
("CONTENT"->>'payment_method')::integer AS payment_method,
("CONTENT"->>'total_value')::numeric AS total_value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_seller')::integer AS id_user_seller
FROM "bendito_intelligence"."bendito"."bdt_raw_invoice_change"
  );
  