
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_goals_invoice_rules__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_goal')::integer AS id_goal,
("CONTENT"->>'seller_ids')::integer[] AS seller_ids,
("CONTENT"->>'team_ids')::integer[] AS team_ids,
("CONTENT"->>'status_invoice')::integer[] AS status_invoice,
("CONTENT"->>'nature_invoice')::integer[] AS nature_invoice,
("CONTENT"->>'price_table_ids')::integer[] AS price_table_ids,
("CONTENT"->>'tags_invoice')::text[] AS tags_invoice,
("CONTENT"->>'invoice_source')::integer AS invoice_source,
("CONTENT"->>'integrated_invoice')::boolean AS integrated_invoice,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_goals_invoice_rules"
  );
  