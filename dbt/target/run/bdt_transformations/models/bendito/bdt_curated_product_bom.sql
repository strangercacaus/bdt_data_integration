
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_product_bom__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_product')::integer AS id_product,
("CONTENT"->>'id_item')::integer AS id_item,
("CONTENT"->>'id_component')::integer AS id_component,
("CONTENT"->>'quantity')::numeric AS quantity,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'description')::text AS description,
("CONTENT"->>'integration_code')::text AS integration_code
FROM "bendito_intelligence"."bendito"."bdt_raw_product_bom"
  );
  