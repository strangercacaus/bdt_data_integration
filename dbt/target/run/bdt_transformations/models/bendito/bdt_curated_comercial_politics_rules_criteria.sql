
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_comercial_politics_rules_criteria__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_rule')::integer AS id_rule,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'values')::text AS values,
("CONTENT"->>'type_signal')::integer AS type_signal,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_comercial_politics_rules_criteria"
  );
  