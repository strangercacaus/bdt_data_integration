
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_goals_client_rules__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_goal')::integer AS id_goal,
("CONTENT"->>'type_client')::integer AS type_client,
("CONTENT"->>'state_ids')::integer[] AS state_ids,
("CONTENT"->>'tags_client')::text[] AS tags_client,
("CONTENT"->>'custom_field_ids')::integer[] AS custom_field_ids,
("CONTENT"->>'custom_field_value_ids')::integer[] AS custom_field_value_ids,
("CONTENT"->>'final_client')::boolean AS final_client,
("CONTENT"->>'resseler')::boolean AS resseler,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_goals_client_rules"
  );
  