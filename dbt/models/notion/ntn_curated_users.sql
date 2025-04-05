{{config(
    materialized = 'table'
)}}
with unique_person as
(
	select distinct jsonb_array_elements(solicitante->'people') as person
	from  {{ref('ntn_processed_universal_task_database')}}
	union all
	select distinct jsonb_array_elements(tester->'people')
	from  {{ref('ntn_processed_universal_task_database')}}
	union all
	select distinct jsonb_array_elements(responsavel->'people')
	from  {{ref('ntn_processed_universal_task_database')}}
)
select distinct
	person ->>'id' as id,
	person ->>'name' as name,
	person -> 'person' ->> 'email' as email,
	person ->>'avatar_url' as avatar
from unique_person