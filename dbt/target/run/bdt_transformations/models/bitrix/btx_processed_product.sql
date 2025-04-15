
  create view "bendito_intelligence"."bitrix"."btx_processed_product__dbt_tmp"
    
    
  as (
    
SELECT
    -- Primary identifiers
    "ID"::int4 AS id,
    ("CONTENT"->>'NAME')::varchar AS name,
    ("CONTENT"->>'CODE')::varchar AS code,
    ("CONTENT"->>'XML_ID')::int2 AS xml_id,
    NULLIF("CONTENT"->>'CATALOG_ID', '')::int2 AS catalog_id,
    ("CONTENT"->>'SECTION_ID')::varchar AS section_id,
    
    -- Product details
    ("CONTENT"->>'DESCRIPTION')::text AS description,
    ("CONTENT"->>'DESCRIPTION_TYPE')::varchar AS description_type,
    ("CONTENT"->>'DETAIL_PICTURE')::text AS detail_picture,
    ("CONTENT"->>'PREVIEW_PICTURE')::text AS preview_picture,
    ("CONTENT"->>'PROPERTY_45')::text AS property_45,
    
    -- Pricing and measurements
    ("CONTENT"->>'CURRENCY_ID')::varchar AS currency_id,
    NULLIF("CONTENT"->>'PRICE','')::numeric AS price,
    NULLIF("CONTENT"->>'MEASURE','')::int2 AS measure,
    NULLIF("CONTENT"->>'VAT_ID', '')::int2 AS vat_id,
    NULLIF(REPLACE("CONTENT"->>'VAT_INCLUDED', '[]', ''), '')::bool AS vat_included,
    
    -- Status and sorting
    NULLIF(REPLACE("CONTENT"->>'ACTIVE', '[]', ''), '')::bool AS active,
    NULLIF("CONTENT"->>'SORT','')::int2 AS sort,
    
    -- Dates and timestamps
    NULLIF("CONTENT"->>'DATE_CREATE', '')::timestamptz AS date_create,
    ("CONTENT"->>'TIMESTAMP_X')::timestamptz AS timestamp_x,
    
    -- User references
    NULLIF("CONTENT"->>'CREATED_BY', '')::int2 AS created_by,
    NULLIF("CONTENT"->>'MODIFIED_BY', '')::int2 AS modified_by
FROM "bendito_intelligence"."bitrix"."btx_raw_product"
WHERE "SUCCESS" = true
  );