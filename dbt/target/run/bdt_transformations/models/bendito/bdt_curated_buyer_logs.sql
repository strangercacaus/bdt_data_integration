
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_buyer_logs__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'timestamp')::timestamp without time zone AS timestamp,
("CONTENT"->>'buyer_id')::integer AS buyer_id,
("CONTENT"->>'customer_id')::integer AS customer_id,
("CONTENT"->>'type')::integer AS type
FROM "bendito_intelligence"."bendito"."bdt_raw_buyer_logs"
  );
  