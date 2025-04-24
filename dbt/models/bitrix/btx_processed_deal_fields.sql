{{ config(
    materialized='table',
    unique_key='id'
) }}

{% set raw = source('bitrix', 'btx_raw_deal_fields') %}

{{ process_jsonb_fields(raw) }}
