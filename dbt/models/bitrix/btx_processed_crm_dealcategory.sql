{% set raw = source('bitrix', 'btx_raw_crm_dealcategory') %}
{{ process_jsonb_fields(raw) }}
