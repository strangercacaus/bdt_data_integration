{{ config(
    materialized='table'
) }}

select 
	"ID" as id,
	("CONTENT"->>'STATUS')::int4 as status,
	("CONTENT"->>'ASSOCIATED_ENTITY_ID')::int4 as associated_entity_id,
	("CONTENT"->>'AUTHOR_ID')::int4 as author_id, 
	("CONTENT"->>'AUTOCOMPLETE_RULE')::int2 as autocomplete_rule,
	("CONTENT"->>'COMPLETED')::bool as completed,
	("CONTENT"->>'CREATED')::timestamptz as created_at,        
	("CONTENT"->>'DEADLINE')::timestamptz as deadline,    
	("CONTENT"->>'DESCRIPTION')::varchar as description,
	("CONTENT"->>'DESCRIPTION_TYPE')::int2 as description_type,
	("CONTENT"->>'DIRECTION')::int2 as direction,
	("CONTENT"->>'EDITOR_ID')::int2 as editor_id,
	(replace("CONTENT"->>'END_TIME','',null))::timestamptz as end_time,
	--("CONTENT"->>'ERROR')::text as error,
	("CONTENT"->>'FILES')::jsonb as files,
	("CONTENT"->>'ID')::int4 as content_id,
	("CONTENT"->>'LAST_UPDATED')::timestamptz as last_updated,
	("CONTENT"->>'LOCATION')::text as location,
	("CONTENT"->>'NOTIFY_TYPE')::int2 as notify_type,
	("CONTENT"->>'NOTIFY_VALUE')::int2 as notify_value,
	("CONTENT"->>'ORIGINATOR_ID')::varchar as originator_id,
	("CONTENT"->>'ORIGIN_ID')::varchar as origin_id,
	("CONTENT"->>'OWNER_ID')::int4 as owner_id,
	("CONTENT"->>'OWNER_TYPE_ID')::int2 as owner_type_id,
	("CONTENT"->>'PRIORITY')::int2 as priority,
	--("CONTENT"->>'PROVIDER_DATA') as provider_data, --sem dados
	--("CONTENT"->>'PROVIDER_GROUP_ID') as provider_group_id, --sem dados
	("CONTENT"->>'PROVIDER_ID')::varchar as provider_id,
	("CONTENT"->>'PROVIDER_PARAMS')::jsonb as provider_params,
	("CONTENT"->>'PROVIDER_TYPE_ID')::varchar as provider_type_id,
	("CONTENT"->>'RESPONSIBLE_ID')::int4 as responsible_id,
	--("CONTENT"->>'RESULT_CURRENCY_ID') as result_currency_id,
	--("CONTENT"->>'RESULT_MARK') as result_mark,
	--("CONTENT"->>'RESULT_SOURCE_ID') as result_source_id,
	--("CONTENT"->>'RESULT_STATUS') as result_status,
	--("CONTENT"->>'RESULT_STREAM') as result_stream,
	--("CONTENT"->>'RESULT_SUM') as result_sum,
	--("CONTENT"->>'RESULT_VALUE') as result_value,
	("CONTENT"->>'SETTINGS')::jsonb as settings,
	(replace("CONTENT"->>'START_TIME','',null))::timestamptz as start_time,
	("CONTENT"->>'STATUS')::int2 as content_status,
	--("CONTENT"->>'STATUS_CODE')::int2 as status_code, -- apenas registros com erro
	("CONTENT"->>'SUBJECT')::text as subject,
	("CONTENT"->>'TYPE_ID')::int2 as type_id,
	("CONTENT"->>'URL')::text as url
from {{ source('bitrix', 't_raw_activity') }};