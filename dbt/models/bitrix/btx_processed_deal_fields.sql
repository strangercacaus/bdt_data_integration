{% set raw = source('bitrix', 'btx_raw_deal_fields') %}
{{ process_jsonb_fields(raw) }}
