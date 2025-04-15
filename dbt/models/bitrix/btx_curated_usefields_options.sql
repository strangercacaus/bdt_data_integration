{{ config(
	materialized = 'table',
	post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ],
)}}
select
	field_name,
	jsonb_array_elements(list)->>'ID' as id,
	jsonb_array_elements(list)->>'VALUE' as value
from
	{{ref('btx_curated_userfields')}}