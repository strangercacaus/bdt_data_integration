
  
    

  create  table "bendito_intelligence_dev"."transformed_transformed"."t_processed_activity__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
  "ID" as id,
  ("CONTENT"->>'SUBJECT')::varchar as subject,
  ("CONTENT"->>'DESCRIPTION')::varchar as description,
  ("CONTENT"->>'TYPE_ID')::varchar as type_id,
  ("CONTENT"->>'PROVIDER_ID')::varchar as provider_id,
  ("CONTENT"->>'PROVIDER_TYPE_ID')::varchar as provider_type_id,
  ("CONTENT"->>'PRIORITY')::varchar as priority,
  ("CONTENT"->>'OWNER_ID')::integer as owner_id,
  ("CONTENT"->>'OWNER_TYPE_ID')::varchar as owner_type_id,
  ("CONTENT"->>'RESPONSIBLE_ID')::integer as responsible_id,
  ("CONTENT"->>'COMPLETED')::boolean as completed,
  ("CONTENT"->>'STATUS')::integer as status,
  ("CONTENT"->>'CREATED')::timestamp as created,
  ("CONTENT"->>'LAST_UPDATED')::timestamp as last_updated
FROM bitrix.t_raw_activity
  );
  