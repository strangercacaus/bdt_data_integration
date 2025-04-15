
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_characteristic_value_dependency_rules__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_dependency')::integer AS id_dependency,
("CONTENT"->>'id_char_condition')::integer AS id_char_condition,
("CONTENT"->>'signal')::integer AS signal,
("CONTENT"->>'values')::json AS values,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_characteristic_value_dependency_rules"
  );
  