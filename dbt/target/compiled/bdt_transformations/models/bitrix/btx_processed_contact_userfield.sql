
select
	"ID" :: int4 as id,
	("CONTENT" ->> 'FIELD_NAME') :: varchar as field_name,
	NULLIF(REPLACE("CONTENT" ->> 'LIST', '''', '"'),'None') :: jsonb as list,
	NULLIF("CONTENT" ->> 'SORT', '') :: int2 as sort,
	NULLIF("CONTENT" ->> 'XML_ID', '') :: text as xml_id,
	NULLIF(REPLACE("CONTENT" ->> 'MULTIPLE','[]',''),'') :: bool as multiple,
	NULLIF(REPLACE("CONTENT" ->> 'SETTINGS', '''', '"'),'None') :: jsonb as settings,
	("CONTENT" ->> 'ENTITY_ID') :: varchar as entity_id,
	NULLIF(REPLACE("CONTENT" ->> 'MANDATORY','[]',''),'')  :: bool as mandatory,
	("CONTENT" ->> 'SHOW_FILTER') :: varchar as show_filter,
	NULLIF(REPLACE("CONTENT" ->> 'EDIT_IN_LIST','[]',''),'') :: bool as edit_in_list,
	NULLIF(REPLACE("CONTENT" ->> 'SHOW_IN_LIST','[]',''),'') :: bool as show_in_list,
	("CONTENT" ->> 'USER_TYPE_ID') :: varchar as user_type_id,
	NULLIF(REPLACE("CONTENT" ->> 'IS_SEARCHABLE','[]',''),'') :: bool as is_searchable,
	NULLIF("CONTENT" ->> 'EDIT_FORM_LABEL', '') :: jsonb as edit_form_label,
	NULLIF("CONTENT" ->> 'ERROR_MESSAGE', '') :: jsonb as error_message,
	NULLIF("CONTENT" ->> 'HELP_MESSAGE', '') :: jsonb as help_message,
	NULLIF("CONTENT" ->> 'LIST_COLUMN_LABEL', '') :: jsonb as list_column_label,
	NULLIF("CONTENT" ->> 'LIST_FILTER_LABEL', '') :: jsonb as list_filter_label
from
	"bendito_intelligence"."bitrix"."btx_raw_contact_userfield"
WHERE
	"SUCCESS" = true