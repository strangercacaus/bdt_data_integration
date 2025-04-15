
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_sequence__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'customer')::integer AS customer,
("CONTENT"->>'object')::character varying AS object,
("CONTENT"->>'value')::integer AS value
FROM "bendito_intelligence"."bendito"."bdt_raw_sequence"
  );
  