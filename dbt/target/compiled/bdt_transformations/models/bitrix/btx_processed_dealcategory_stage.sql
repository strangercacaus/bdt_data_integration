
SELECT
  "ID" as id,
  ("CONTENT" ->> 'NAME') :: varchar as name,
  NULLIF("CONTENT" ->> 'SORT', '') :: int4 as sort,
  ("CONTENT" ->> 'STATUS_ID') :: varchar as status_id
from
  "bendito_intelligence"."bitrix"."btx_raw_dealcategory_stage"
WHERE
  "SUCCESS" = true