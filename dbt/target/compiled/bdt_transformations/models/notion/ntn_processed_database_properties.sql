
select distinct
	"CONTENT" -> 'parent' ->> 'database_id' as database_id,
	'ntn_raw_universal_task_database' as database_name,
	to_jsonb(jsonb_each("CONTENT"->'properties'))->'value'->>'id' as "property_id",
	to_jsonb(jsonb_each("CONTENT"->'properties'))->>'key' as "property_name",
	to_jsonb(jsonb_each("CONTENT"->'properties'))->'value'->>'type' as "property_type"
from "bendito_intelligence"."notion"."ntn_raw_universal_task_database"
union all
select distinct
	"CONTENT" -> 'parent' ->> 'database_id' as database_id,
	'ntn_raw_action_items' as database_name,
	to_jsonb(jsonb_each("CONTENT"->'properties'))->'value'->>'id' as "property_id",
	to_jsonb(jsonb_each("CONTENT"->'properties'))->>'key' as "property_name",
	to_jsonb(jsonb_each("CONTENT"->'properties'))->'value'->>'type' as "property_type"
from "bendito_intelligence"."notion"."ntn_raw_action_items"