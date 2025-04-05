
  create view "bendito_intelligence_dev"."bitrix"."btx_processed_product__dbt_tmp"
    
    
  as (
    
SELECT
  "ID" :: int4 as id,
  ("CONTENT" ->> 'NAME') :: varchar as name,
  NULLIF("CONTENT" ->> 'CREATED_BY', '') :: int2 as created_by,
  NULLIF("CONTENT" ->> 'DATE_CREATE', '') :: timestamptz as date_create,
  NULLIF("CONTENT" ->> 'MODIFIED_BY', '') :: int2 as modified_by,
  NULLIF(REPLACE("CONTENT" ->> 'ACTIVE', '[]', ''), '') :: bool as active,
  NULLIF("CONTENT" ->> 'CATALOG_ID', '') :: int2 as catalog_id,
  ("CONTENT" ->> 'CODE') :: varchar as code,
  ("CONTENT" ->> 'CURRENCY_ID') :: varchar as currency_id,
  ("CONTENT" ->> 'DESCRIPTION') :: text as description,
  ("CONTENT" ->> 'DESCRIPTION_TYPE') :: varchar as description_type,
  ("CONTENT" ->> 'DETAIL_PICTURE') :: text as detail_picture,
  ("CONTENT" ->> 'PREVIEW_PICTURE') :: text as preview_picture,
  NULLIF("CONTENT" ->> 'MEASURE','') :: int2 as measure,
  NULLIF("CONTENT" ->> 'PRICE','') :: numeric as price,
  ("CONTENT" ->> 'PROPERTY_45') :: text as property_45,
  ("CONTENT" ->> 'SECTION_ID') :: varchar as section_id,
  NULLIF("CONTENT" ->> 'SORT','') :: int2 as sort,
  ("CONTENT" ->> 'TIMESTAMP_X') :: timestamptz as timestamp_x,
  NULLIF("CONTENT" ->> 'VAT_ID', '') :: int2 as vat_id,
  NULLIF(REPLACE("CONTENT" ->> 'VAT_INCLUDED', '[]', ''), '') :: bool as vat_included,
  ("CONTENT" ->> 'XML_ID') :: int2 as xml_id
FROM
  "bendito_intelligence_dev"."bitrix"."btx_raw_product"
WHERE
  "SUCCESS" = true
  );