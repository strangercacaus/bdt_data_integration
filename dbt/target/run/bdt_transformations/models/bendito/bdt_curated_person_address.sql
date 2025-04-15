
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_person_address__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_person')::integer AS id_person,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'cep')::character varying AS cep,
("CONTENT"->>'address')::character varying AS address,
("CONTENT"->>'number')::character varying AS number,
("CONTENT"->>'complement')::character varying AS complement,
("CONTENT"->>'neighborhood')::character varying AS neighborhood,
("CONTENT"->>'city')::character varying AS city,
("CONTENT"->>'state')::integer AS state,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'reference')::text AS reference,
("CONTENT"->>'is_from_importation')::boolean AS is_from_importation,
("CONTENT"->>'person_address')::character varying AS person_address,
("CONTENT"->>'integration_code')::character varying AS integration_code
FROM "bendito_intelligence"."bendito"."bdt_raw_person_address"
  );
  