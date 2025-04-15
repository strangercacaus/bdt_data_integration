
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_anp__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'anp_code')::character varying AS anp_code,
("CONTENT"->>'description')::character varying AS description
FROM "bendito_intelligence"."bendito"."bdt_raw_anp"
  );
  