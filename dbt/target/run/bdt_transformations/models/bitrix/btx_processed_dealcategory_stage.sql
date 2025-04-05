
  create view "bendito_intelligence_dev"."bitrix"."btx_processed_dealcategory_stage__dbt_tmp"
    
    
  as (
    
SELECT
  "ID" as id,
  ("CONTENT" ->> 'NAME') :: varchar as name,
  NULLIF("CONTENT" ->> 'SORT', '') :: int4 as sort,
  ("CONTENT" ->> 'STATUS_ID') :: varchar as status_id
from
  "bendito_intelligence_dev"."bitrix"."btx_raw_dealcategory_stage"
WHERE
  "SUCCESS" = true
  );