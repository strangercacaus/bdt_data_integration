
select
	field_name,
	jsonb_array_elements(list)->>'ID' as id,
	jsonb_array_elements(list)->>'VALUE' as value
from
	"bendito_intelligence"."bitrix"."btx_curated_userfields"