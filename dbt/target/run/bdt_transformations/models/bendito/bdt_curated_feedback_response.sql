
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_feedback_response__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'customer')::integer AS customer,
("CONTENT"->>'user')::integer AS user,
("CONTENT"->>'question')::integer AS question,
("CONTENT"->>'answer')::integer AS answer,
("CONTENT"->>'skipped')::boolean AS skipped,
("CONTENT"->>'obs')::text AS obs,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_feedback_response"
  );
  