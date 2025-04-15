
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_classes_characteristic__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_class')::integer AS id_class,
("CONTENT"->>'id_characteristic')::integer AS id_characteristic,
("CONTENT"->>'required')::boolean AS required,
("CONTENT"->>'order')::integer AS order,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'append_description')::boolean AS append_description
FROM "bendito_intelligence"."bendito"."bdt_raw_classes_characteristic"
  );
  