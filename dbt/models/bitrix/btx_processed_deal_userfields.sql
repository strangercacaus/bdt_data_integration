{ { config(materialized = 'view', unique_key = 'id',) } }
select
	"ID" :: int4 as id,
	("CONTENT" ->> 'FIELD_NAME') :: varchar as field_name,
	(("CONTENT" ->> 'EDIT_FORM_LABEL') :: jsonb) ->> 'br' as friendly_name,
	jsonb_path_query_array(
		replace(
			replace("CONTENT" ->> 'LIST', '''', '"'),
			'None',
			'null'
		) :: jsonb,
		'$[*].VALUE'
	) as options,
	replace(
		replace("CONTENT" ->> 'LIST', '''', '"'),
		'None',
		'null'
	) :: jsonb as list,
	("CONTENT" ->> 'SORT') :: int2 as sort,
	("CONTENT" ->> 'XML_ID') :: text as xml_id,
	("CONTENT" ->> 'MULTIPLE') :: bool as multiple,
	replace(
		replace("CONTENT" ->> 'SETTINGS', '''', '"'),
		'None',
		'null'
	) :: jsonb as settings,
	("CONTENT" ->> 'ENTITY_ID') :: varchar as entity_id,
	("CONTENT" ->> 'MANDATORY') :: bool as mandatory,
	("CONTENT" ->> 'SHOW_FILTER') :: varchar as show_filter,
	("CONTENT" ->> 'EDIT_IN_LIST') :: bool as edit_in_list,
	("CONTENT" ->> 'SHOW_IN_LIST') :: bool as show_in_list,
	("CONTENT" ->> 'USER_TYPE_ID') :: varchar as user_type_id,
	("CONTENT" ->> 'IS_SEARCHABLE') :: bool as is_searchable,
	("CONTENT" ->> 'EDIT_FORM_LABEL') :: jsonb as edit_form_label,
	("CONTENT" ->> 'ERROR_MESSAGE') :: jsonb as error_message,
	("CONTENT" ->> 'HELP_MESSAGE') :: jsonb as help_message,
	("CONTENT" ->> 'LIST_COLUMN_LABEL') :: jsonb as list_column_label,
	("CONTENT" ->> 'LIST_FILTER_LABEL') :: jsonb as list_filter_label
from
	{ { source('bitrix', 'btx_raw_deal_userfield') } }
WHERE
	"SUCCESS" = true