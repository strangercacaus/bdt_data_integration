{ { config(materialized = 'view',) } }
SELECT
  "ID" :: int4 AS id,
  ("CONTENT" ->> 'SUBJECT') :: VARCHAR AS subject,
  ("CONTENT" ->> 'DESCRIPTION') :: VARCHAR AS description,
  ("CONTENT" ->> 'TYPE_ID') :: VARCHAR AS type_id,
  ("CONTENT" ->> 'PROVIDER_ID') :: VARCHAR AS provider_id,
  ("CONTENT" ->> 'PROVIDER_TYPE_ID') :: VARCHAR AS provider_type_id,
  ("CONTENT" ->> 'PRIORITY') :: VARCHAR AS priority,
  ("CONTENT" ->> 'OWNER_ID') :: INTEGER AS owner_id,
  ("CONTENT" ->> 'OWNER_TYPE_ID') :: VARCHAR AS owner_type_id,
  ("CONTENT" ->> 'RESPONSIBLE_ID') :: INTEGER AS responsible_id,
  ("CONTENT" ->> 'COMPLETED') :: BOOLEAN AS completed,
  ("CONTENT" ->> 'STATUS') :: INTEGER AS status,
  ("CONTENT" ->> 'CREATED') :: TIMESTAMP AS created,
  ("CONTENT" ->> 'LAST_UPDATED') :: TIMESTAMP AS last_updated
FROM
  { { source('bitrix', 'btx_raw_activity') } }
WHERE
  "SUCCESS" = TRUE