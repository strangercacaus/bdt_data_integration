
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_custom_field_entity__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'values')::text AS values,
("CONTENT"->>'entity_id')::integer AS entity_id,
("CONTENT"->>'custom_field_id')::integer AS custom_field_id
FROM "bendito_intelligence"."bendito"."bdt_raw_custom_field_entity"
  );
  