
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_approver__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_workflow')::integer AS id_workflow,
("CONTENT"->>'id_user')::integer AS id_user,
("CONTENT"->>'level')::integer AS level,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_approver"
  );
  