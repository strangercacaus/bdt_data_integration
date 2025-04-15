
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_services_taxation__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'code')::character varying AS code,
("CONTENT"->>'description')::character varying AS description
FROM "bendito_intelligence"."bendito"."bdt_raw_services_taxation"
  );
  