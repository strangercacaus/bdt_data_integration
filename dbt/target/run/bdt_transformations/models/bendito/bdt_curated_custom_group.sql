
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_custom_group__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'name')::character varying AS name,
("CONTENT"->>'entity')::integer AS entity,
("CONTENT"->>'order')::integer AS order,
("CONTENT"->>'customer_id')::integer AS customer_id
FROM "bendito_intelligence"."bendito"."bdt_raw_custom_group"
  );
  