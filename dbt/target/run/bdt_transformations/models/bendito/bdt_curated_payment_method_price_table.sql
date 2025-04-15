
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_payment_method_price_table__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'price_table_id')::integer AS price_table_id,
("CONTENT"->>'payment_method_id')::integer AS payment_method_id
FROM "bendito_intelligence"."bendito"."bdt_raw_payment_method_price_table"
  );
  