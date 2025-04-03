
  
    

  create  table "bendito_intelligence_dev"."transformed_transformed"."t_processed_deal_stage__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
  "ID" as id,
  ("CONTENT"->>'NAME')::varchar as name,
  ("CONTENT"->>'SORT')::int4 as sort,
  ("CONTENT"->>'STATUS_ID')::varchar as status_id
FROM bitrix.t_raw_deal_stage
  );
  