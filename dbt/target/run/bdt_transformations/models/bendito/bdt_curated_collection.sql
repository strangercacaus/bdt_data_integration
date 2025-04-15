
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_collection__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_collection')::integer AS id_collection,
("CONTENT"->>'detailed_description')::character varying AS detailed_description,
("CONTENT"->>'image')::text AS image,
("CONTENT"->>'description_2')::character varying AS description_2,
("CONTENT"->>'detailed_description_2')::character varying AS detailed_description_2,
("CONTENT"->>'description_3')::character varying AS description_3,
("CONTENT"->>'detailed_description_3')::character varying AS detailed_description_3,
("CONTENT"->>'b2b_sale')::boolean AS b2b_sale,
("CONTENT"->>'app_sale')::boolean AS app_sale
FROM "bendito_intelligence"."bendito"."bdt_raw_collection"
  );
  