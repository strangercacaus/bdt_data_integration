select
	"ID" :: int4 as id,
	NULLIF("CONTENT" ->> 'SORT', '') :: int2 as sort,
	("CONTENT" ->> 'NAME') :: varchar as name,
	NULLIF("CONTENT" ->> 'STATUS_ID','') :: varchar as status_id
from
	{{ source('bitrix', 'btx_raw_crm_dealcategory_stage') }}
WHERE
	"SUCCESS" = true