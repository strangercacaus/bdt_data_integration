{{ config(
    materialized='incremental',
    unique_key='id',
) }}

SELECT 
  "ID" as id,
  ("CONTENT"->>'NAME')::varchar as name,
  ("CONTENT"->>'SORT')::int4 as sort,
  ("CONTENT"->>'STATUS_ID')::varchar as status_id
FROM raw.btx_raw_deal_stage 