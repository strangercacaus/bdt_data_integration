
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_job__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'description')::character varying AS description,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'records_to_process')::integer AS records_to_process,
("CONTENT"->>'records_processed')::integer AS records_processed,
("CONTENT"->>'pages_to_process')::integer AS pages_to_process,
("CONTENT"->>'started_processing_at')::timestamp without time zone AS started_processing_at,
("CONTENT"->>'finished_processing_at')::timestamp without time zone AS finished_processing_at,
("CONTENT"->>'continue_last_job')::boolean AS continue_last_job
FROM "bendito_intelligence"."bendito"."bdt_raw_job"
  );
  