
  create view "bendito_intelligence"."bitrix"."btx_processed_company__dbt_tmp"
    
    
  as (
    
select 
    -- Primary identifiers
    "ID"::int2 as id,
    ("CONTENT"->>'TITLE')::varchar AS title,
    ("CONTENT"->>'COMPANY_TYPE')::varchar AS company_type,
    ("CONTENT"->>'INDUSTRY')::varchar AS industry,
    ("CONTENT"->>'EMPLOYEES')::varchar AS employees,
    ("CONTENT"->>'CURRENCY_ID')::varchar AS currency_id,
    ("CONTENT"->>'REVENUE')::numeric AS revenue,
    
    -- Contact information
    ("CONTENT"->>'WEB')::jsonb AS web,
    NULLIF("CONTENT"->>'HAS_EMAIL','[]')::bool AS has_email,
    NULLIF("CONTENT"->>'HAS_PHONE','[]')::bool AS has_phone,
    
    -- Address information
    ("CONTENT"->>'ADDRESS_2')::text AS address_2,
    
    -- Status and activity
    NULLIF("CONTENT"->>'OPENED','[]')::bool AS opened,
    NULLIF("CONTENT"->>'IS_MY_COMPANY','[]')::bool AS is_my_company,
    ("CONTENT"->>'COMMENTS')::text AS comments,
    
    -- Dates and timestamps
    NULLIF("CONTENT"->>'DATE_CREATE','')::timestamptz AS date_create,
    NULLIF("CONTENT"->>'DATE_MODIFY','')::timestamptz AS date_modify,
    NULLIF("CONTENT"->>'LAST_ACTIVITY_TIME','')::timestamptz AS last_activity_time,
    
    -- User references
    NULLIF("CONTENT"->>'CREATED_BY_ID','')::int2 AS created_by_id,
    NULLIF("CONTENT"->>'MODIFY_BY_ID','')::int2 AS modify_by_id,
    NULLIF("CONTENT"->>'ASSIGNED_BY_ID','')::int2 AS assigned_by_id,
    NULLIF("CONTENT"->>'LAST_ACTIVITY_BY','')::int2 AS last_activity_by,
    
    -- Source and origin
    ("CONTENT"->>'ORIGIN_ID')::varchar AS origin_id,
    NULLIF("CONTENT"->>'LEAD_ID','')::int2 AS lead_id,
    
    -- Custom fields (UF_CRM)
    ("CONTENT"->>'UF_CRM_1724941360')::varchar AS uf_crm_1724941360,
    ("CONTENT"->>'UF_CRM_1724941486')::varchar AS uf_crm_1724941486,
    ("CONTENT"->>'UF_CRM_1724941613')::varchar AS uf_crm_1724941613,
    ("CONTENT"->>'UF_CRM_1720548388015')::varchar AS uf_crm_1720548388015,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726169541171','false',''),'')::varchar AS uf_crm_1726169541171,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726169579580','false',''),'')::int2 AS uf_crm_1726169579580,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726169611418','false',''),'')::int2 AS uf_crm_1726169611418,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726169646823','false',''),'')::varchar AS uf_crm_1726169646823,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726169666589','false',''),'')::varchar AS uf_crm_1726169666589,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726173756398','false',''),'')::varchar AS uf_crm_1726173756398,
    NULLIF(REPLACE(REPLACE("CONTENT"->>'UF_CRM_1726173773647','false',''),'#N/A',''),'')::int4 AS uf_crm_1726173773647,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_1726940183689','false',''),'')::varchar AS uf_crm_1726940183689,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_COMPANY_1720014986901','false',''),'')::varchar AS uf_crm_company_1720014986901,
    NULLIF(REPLACE("CONTENT"->>'UF_CRM_COMPANY_1738069980122','false',''),'')::int2 AS uf_crm_company_1738069980122
from "bendito_intelligence"."bitrix"."btx_raw_company"
WHERE "SUCCESS" = true
  );