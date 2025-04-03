
      
  
    

  create  table "bendito_intelligence_dev"."processed_bitrix"."btx_processed_dealcategory"
  
  
    as
  
  (
    

SELECT 
  "ID" as id,
  ("CONTENT"->>'NAME')::varchar as name,
  ("CONTENT"->>'SORT')::int4 as sort,
  ("CONTENT"->>'CREATED_DATE')::timestamp as created_date
FROM bitrix.btx_raw_dealcategory
  );
  
  