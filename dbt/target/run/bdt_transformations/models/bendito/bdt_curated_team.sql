
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_team__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_team"
  );
  