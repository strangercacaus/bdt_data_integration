
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_buyer__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'username')::character varying AS username,
("CONTENT"->>'password')::character varying AS password,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'first_name')::character varying AS first_name,
("CONTENT"->>'last_name')::character varying AS last_name,
("CONTENT"->>'nick_name')::character varying AS nick_name,
("CONTENT"->>'birthday')::date AS birthday,
("CONTENT"->>'gender')::integer AS gender,
("CONTENT"->>'phone_prefix')::integer AS phone_prefix,
("CONTENT"->>'phone_number')::integer AS phone_number,
("CONTENT"->>'cellphone_prefix')::integer AS cellphone_prefix,
("CONTENT"->>'cellphone_number')::integer AS cellphone_number,
("CONTENT"->>'validated')::boolean AS validated,
("CONTENT"->>'last_access')::timestamp without time zone AS last_access,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'image')::character varying AS image,
("CONTENT"->>'active_newsletter')::boolean AS active_newsletter,
("CONTENT"->>'federal_registration')::character varying AS federal_registration
FROM "bendito_intelligence"."bendito"."bdt_raw_buyer"
  );
  