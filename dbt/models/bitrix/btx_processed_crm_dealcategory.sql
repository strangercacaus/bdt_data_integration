{% set raw = source('bitrix', 'btx_raw_dealcategory') %}
{{ process_jsonb_fields(raw) }}
