
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_plans__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'user_control')::boolean AS user_control,
("CONTENT"->>'user_number')::integer AS user_number,
("CONTENT"->>'transational')::boolean AS transational,
("CONTENT"->>'code')::character varying AS code
FROM "bendito_intelligence"."bendito"."bdt_raw_plans"
  );
  