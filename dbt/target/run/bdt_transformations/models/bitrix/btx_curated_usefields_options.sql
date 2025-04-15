
  
    

  create  table "bendito_intelligence"."bitrix"."btx_curated_usefields_options__dbt_tmp"
  
  
    as
  
  (
    
select
	field_name,
	jsonb_array_elements(list)->>'ID' as id,
	jsonb_array_elements(list)->>'VALUE' as value
from
	"bendito_intelligence"."bitrix"."btx_curated_userfields"
  );
  