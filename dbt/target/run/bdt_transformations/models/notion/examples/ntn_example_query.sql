
  
    

  create  table "bendito_intelligence_dev"."notion"."ntn_example_query__dbt_tmp"
  
  
    as
  
  (
    /*
This is an example query showing how to use the processed Notion Universal Task Database
Specifically, it demonstrates how to work with the solicitante_ids field
*/

select
	page_id,
	emoji ->> 'emoji' as emoji,
	created_by ->> 'id' as created_by_id,
	last_edited_by ->> 'id' as last_edited_by_id,
	created_time,
	last_edited_time,
	
	-- Using the new solicitante_ids field to access requester IDs
	solicitante_ids, -- This is already an array of IDs as jsonb
	
	-- Getting the first requester ID (if it exists)
	solicitante_ids->0 as first_requester_id,
	
	-- Counting how many requesters there are
	jsonb_array_length(solicitante_ids) as requester_count,
	
	-- For reference only: original JSON structure
	solicitante,
	
	-- Other fields from the model
	prioridade,
	status,
	titulo
from
	"bendito_intelligence_dev"."notion"."ntn_processed_universal_task_database"
where
	-- Example: filter for only tasks with at least one requester
	jsonb_array_length(solicitante_ids) > 0
limit 10;
  );
  