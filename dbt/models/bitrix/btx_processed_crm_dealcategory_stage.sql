{% set raw = source('bitrix', 'btx_raw_dealcategory_stage') %}
{{ process_jsonb_fields(raw) }}