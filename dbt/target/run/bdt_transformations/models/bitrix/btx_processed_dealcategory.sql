
  create view "bendito_intelligence"."bitrix"."btx_processed_dealcategory__dbt_tmp"
    
    
  as (
    
SELECT
  "ID" as id,
  ("CONTENT" ->> 'NAME') :: varchar as name,
  NULLIF("CONTENT" ->> 'SORT', '') :: int4 as sort,
  NULLIF("CONTENT" ->> 'CREATED_DATE', '') :: timestamp as created_date
from
  "bendito_intelligence"."bitrix"."btx_raw_dealcategory"
WHERE
  "SUCCESS" = true
  );