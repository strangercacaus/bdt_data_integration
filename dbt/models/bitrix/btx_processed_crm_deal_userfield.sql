{% set raw = source('bitrix', 'btx_raw_deal_userfield') %}
{{ process_jsonb_fields(raw) }}