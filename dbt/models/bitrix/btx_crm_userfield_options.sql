{{ config(
	materialized = 'table',
	post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ],
)}}
select
	field_name,
	nome_de_exibicao,
	(jsonb_array_elements(list)->>'ID')::int4 as id,
	jsonb_array_elements(list)->>'VALUE' as value
from
	{{ref('btx_crm_userfield')}}