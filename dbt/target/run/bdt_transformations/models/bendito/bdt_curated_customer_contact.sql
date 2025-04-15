
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_customer_contact__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'contact')::character varying AS contact,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'name')::character varying AS name,
("CONTENT"->>'obs')::text AS obs,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_customer_contact"
  );
  