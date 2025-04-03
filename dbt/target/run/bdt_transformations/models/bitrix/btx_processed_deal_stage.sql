
      
  
    

  create  table "bendito_intelligence_dev"."processed_bitrix"."btx_processed_deal_stage"
  
  
    as
  
  (
    

SELECT 
  "ID" as id,
  ("CONTENT"->>'NAME')::varchar as name,
  ("CONTENT"->>'SORT')::int4 as sort,
  ("CONTENT"->>'STATUS_ID')::varchar as status_id
FROM bitrix.btx_raw_deal_stage
  );
  
  