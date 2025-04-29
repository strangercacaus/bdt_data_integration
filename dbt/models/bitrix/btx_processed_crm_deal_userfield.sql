{% set raw = source('bitrix', 'btx_raw_crm_deal_userfield') %}
{{ process_jsonb_fields(raw) }}