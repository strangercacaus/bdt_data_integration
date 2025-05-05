select
	"ID" :: int4 as id,
	NULLIF("CONTENT" ->> 'created_date', '') :: timestamptz as created_date,
	NULLIF("CONTENT" ->> 'SORT', '') :: int2 as sort,
	("CONTENT" ->> 'NAME') :: varchar as name,
	NULLIF(REPLACE("CONTENT" ->> 'IS_LOCKED','[]',''),'') :: bool as is_locked
from
	{{ source('bitrix', 'btx_raw_crm_dealcategory') }}
WHERE
	"SUCCESS" = true