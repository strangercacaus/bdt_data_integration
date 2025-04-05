
SELECT
  "ID" as id,
  ("CONTENT" ->> 'NAME') :: varchar as name,
  NULLIF("CONTENT" ->> 'SORT', '') :: int4 as sort,
  ("CONTENT" ->> 'STATUS_ID') :: varchar as status_id
from
  "bendito_intelligence_dev"."bitrix"."btx_raw_deal_stage"
WHERE
  "SUCCESS" = true