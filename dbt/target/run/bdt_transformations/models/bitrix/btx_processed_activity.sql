
  create view "bendito_intelligence_dev"."bitrix"."btx_processed_activity__dbt_tmp"
    
    
  as (
    
SELECT
  "ID" :: int4 AS id,
  ("CONTENT" ->> 'SUBJECT') AS subject,
  ("CONTENT" ->> 'DESCRIPTION') AS description,
  ("CONTENT" ->> 'TYPE_ID') AS type_id,
  ("CONTENT" ->> 'PROVIDER_ID') AS provider_id,
  ("CONTENT" ->> 'PROVIDER_TYPE_ID') AS provider_type_id,
  ("CONTENT" ->> 'PRIORITY') AS priority,
  NULLIF("CONTENT" ->> 'OWNER_ID', '') :: INTEGER AS owner_id,
  ("CONTENT" ->> 'OWNER_TYPE_ID') AS owner_type_id,
  NULLIF("CONTENT" ->> 'RESPONSIBLE_ID', '') :: INTEGER AS responsible_id,
  NULLIF("CONTENT" ->> 'COMPLETED', '[]') :: BOOLEAN AS completed,
  NULLIF("CONTENT" ->> 'STATUS', '') :: INTEGER AS status,
  NULLIF("CONTENT" ->> 'CREATED', '') :: TIMESTAMP AS created,
  NULLIF("CONTENT" ->> 'LAST_UPDATED', '') :: TIMESTAMP AS last_updated,
  ("CONTENT" ->> 'PROVIDER_GROUP_ID') AS provider_group_id,
  ("CONTENT" ->> 'NOTIFY_VALUE') AS notify_value,
  ("CONTENT" ->> 'EDITOR_ID') AS editor_id,
  ("CONTENT" ->> 'PROVIDER_PARAMS') AS provider_params,
  ("CONTENT" ->> 'LOCATION') AS location,
  ("CONTENT" ->> 'DIRECTION') AS direction,
  ("CONTENT" ->> 'DESCRIPTION_TYPE') AS description_type,
  ("CONTENT" ->> 'DEADLINE') AS deadline,
  ("CONTENT" ->> 'FILES') AS files,
  ("CONTENT" ->> 'RESULT_SOURCE_ID') AS result_source_id,
  ("CONTENT" ->> 'ERROR') AS error,
  ("CONTENT" ->> 'ASSOCIATED_ENTITY_ID') AS associated_entity_id,
  ("CONTENT" ->> 'END_TIME') AS end_time,
  ("CONTENT" ->> 'START_TIME') AS start_time,
  ("CONTENT" ->> 'AUTOCOMPLETE_RULE') AS autocomplete_rule,
  ("CONTENT" ->> 'RESULT_CURRENCY_ID') AS result_currency_id,
  ("CONTENT" ->> 'PROVIDER_DATA') AS provider_data,
  ("CONTENT" ->> 'STATUS_CODE') AS status_code,
  ("CONTENT" ->> 'ORIGIN_ID') AS origin_id,
  ("CONTENT" ->> 'RESULT_SUM') AS result_sum,
  ("CONTENT" ->> 'URL') AS url,
  ("CONTENT" ->> 'RESULT_STATUS') AS result_status,
  ("CONTENT" ->> 'NOTIFY_TYPE') AS notify_type,
  ("CONTENT" ->> 'AUTHOR_ID') AS author_id,
  ("CONTENT" ->> 'RESULT_MARK') AS result_mark,
  ("CONTENT" ->> 'RESULT_STREAM') AS result_stream,
  ("CONTENT" ->> 'SETTINGS') AS settings,
  ("CONTENT" ->> 'ORIGINATOR_ID') AS originator_id,
  ("CONTENT" ->> 'RESULT_VALUE') AS result_value
FROM
  "bendito_intelligence_dev"."bitrix"."btx_raw_activity"
WHERE
  "SUCCESS" = TRUE
  );