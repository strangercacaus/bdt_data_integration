
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_comercial_politics__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'id_politic')::integer AS id_politic,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'start')::timestamp without time zone AS start,
("CONTENT"->>'finish')::timestamp without time zone AS finish,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'editable')::boolean AS editable,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'activate_flex_balance')::boolean AS activate_flex_balance
FROM "bendito_intelligence"."bendito"."bdt_raw_comercial_politics"
  );
  