
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_route_execution__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'start_date')::timestamp without time zone AS start_date,
("CONTENT"->>'end_date')::timestamp without time zone AS end_date,
("CONTENT"->>'ids_clients')::integer[] AS ids_clients,
("CONTENT"->>'id_route')::integer AS id_route,
("CONTENT"->>'id_owner')::integer AS id_owner,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_route_execution"
  );
  