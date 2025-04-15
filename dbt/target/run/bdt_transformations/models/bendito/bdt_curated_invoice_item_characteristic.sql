
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_invoice_item_characteristic__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id_item')::integer AS id_item,
("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_characteristic')::integer AS id_characteristic,
("CONTENT"->>'id_value')::integer AS id_value,
("CONTENT"->>'value')::character varying AS value,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM "bendito_intelligence"."bendito"."bdt_raw_invoice_item_characteristic"
  );
  