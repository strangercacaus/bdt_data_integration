

select 
    'ntn_raw_universal_task_database' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence"."notion"."ntn_raw_universal_task_database"
where "SUCCESS" = FALSE

union all

select
    'ntn_raw_action_items' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence"."notion"."ntn_raw_action_items"
where "SUCCESS" = FALSE