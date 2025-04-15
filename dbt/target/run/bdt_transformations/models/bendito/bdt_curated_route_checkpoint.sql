
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_route_checkpoint__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'position')::integer AS position,
("CONTENT"->>'id_route')::integer AS id_route,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_route_checkpoint"
  );
  