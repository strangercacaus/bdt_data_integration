SELECT
    -- Primary identifiers
    "ID"::int4 AS id,
    ("CONTENT"->>'SUBJECT') AS subject,
    ("CONTENT"->>'DESCRIPTION') AS description,
    ("CONTENT"->>'TYPE_ID') AS type_id,
    
    -- Provider information
    ("CONTENT"->>'PROVIDER_ID') AS provider_id,
    ("CONTENT"->>'PROVIDER_TYPE_ID') AS provider_type_id,
    ("CONTENT"->>'PROVIDER_GROUP_ID') AS provider_group_id,
    ("CONTENT"->>'PROVIDER_PARAMS')::jsonb AS provider_params,
    ("CONTENT"->>'PROVIDER_DATA') AS provider_data,
    
    -- Ownership and responsibility
    NULLIF("CONTENT"->>'OWNER_ID', '')::INTEGER AS owner_id,
    ("CONTENT"->>'OWNER_TYPE_ID') AS owner_type_id,
    NULLIF("CONTENT"->>'RESPONSIBLE_ID', '')::INTEGER AS responsible_id,
    NULLIF("CONTENT"->>'EDITOR_ID','')::int2 AS editor_id,
    ("CONTENT"->>'AUTHOR_ID') AS author_id,
    
    -- Status and completion
    NULLIF("CONTENT"->>'COMPLETED', '[]')::BOOLEAN AS completed,
    NULLIF("CONTENT"->>'STATUS', '')::INTEGER AS status,
    ("CONTENT"->>'STATUS_CODE') AS status_code,
    ("CONTENT"->>'PRIORITY') AS priority,
    ("CONTENT"->>'AUTOCOMPLETE_RULE') AS autocomplete_rule,
    
    -- Dates and timestamps
    NULLIF("CONTENT"->>'CREATED', '')::TIMESTAMP AS created,
    NULLIF("CONTENT"->>'LAST_UPDATED', '')::TIMESTAMP AS last_updated,
    NULLIF("CONTENT"->>'DEADLINE','')::TIMESTAMP AS deadline,
    ("CONTENT"->>'END_TIME') AS end_time,
    ("CONTENT"->>'START_TIME') AS start_time,
    
    -- Results and outcomes
    ("CONTENT"->>'RESULT_SOURCE_ID') AS result_source_id,
    ("CONTENT"->>'RESULT_CURRENCY_ID') AS result_currency_id,
    ("CONTENT"->>'RESULT_SUM') AS result_sum,
    ("CONTENT"->>'RESULT_STATUS') AS result_status,
    ("CONTENT"->>'RESULT_MARK') AS result_mark,
    ("CONTENT"->>'RESULT_STREAM') AS result_stream,
    ("CONTENT"->>'RESULT_VALUE') AS result_value,
    
    -- Additional metadata
    ("CONTENT"->>'LOCATION') AS location,
    NULLIF("CONTENT"->>'DIRECTION','')::INT2 AS direction,
    NULLIF("CONTENT"->>'DESCRIPTION_TYPE','')::INT2 AS description_type,
    ("CONTENT"->'FILES') AS files,
    ("CONTENT"->>'ERROR') AS error,
    ("CONTENT"->>'ASSOCIATED_ENTITY_ID') AS associated_entity_id,
    ("CONTENT"->>'URL') AS url,
    ("CONTENT"->>'SETTINGS') AS settings,
    
    -- Notifications
    NULLIF("CONTENT"->>'NOTIFY_VALUE','')::int2 AS notify_value,
    ("CONTENT"->>'NOTIFY_TYPE') AS notify_type,
    
    -- Source and origin
    ("CONTENT"->>'ORIGIN_ID') AS origin_id,
    ("CONTENT"->>'ORIGINATOR_ID') AS originator_id
FROM {{ source('bitrix', 'btx_raw_crm_activity') }}
WHERE "SUCCESS" = TRUE