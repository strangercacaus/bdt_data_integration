SELECT
    -- Primary identifiers
    "ID"::int2 AS id,
    ("CONTENT"->>'NAME')::varchar AS name,
    ("CONTENT"->>'LAST_NAME')::varchar AS last_name,
    ("CONTENT"->>'SECOND_NAME')::varchar AS second_name,
    ("CONTENT"->>'HONORIFIC')::varchar AS honorific,
    ("CONTENT"->>'POST')::varchar AS post,
    ("CONTENT"->>'BIRTHDATE') AS birthdate,
    ("CONTENT"->>'PHOTO')::text AS photo,
    ("CONTENT"->>'FACE_ID')::text AS face_id,
    
    -- Contact information
    ("CONTENT"->'EMAIL') AS email,
    ("CONTENT"->'PHONE') AS phone,
    ("CONTENT"->>'HAS_EMAIL')::bool AS has_email,
    ("CONTENT"->>'HAS_PHONE')::bool AS has_phone,
    ("CONTENT"->>'HAS_IMOL')::bool AS has_imol,
    
    -- Address information
    ("CONTENT"->>'ADDRESS')::varchar AS address,
    ("CONTENT"->>'ADDRESS_2')::varchar AS address_2,
    ("CONTENT"->>'ADDRESS_CITY') AS address_city,
    ("CONTENT"->>'ADDRESS_COUNTRY') AS address_country,
    ("CONTENT"->>'ADDRESS_LOC_ADDR_ID') AS address_loc_addr_id,
    ("CONTENT"->>'ADDRESS_POSTAL_CODE') AS address_postal_code,
    ("CONTENT"->>'ADDRESS_PROVINCE') AS address_province,
    ("CONTENT"->>'ADDRESS_REGION') AS address_region,
    
    -- Company and lead information
    ("CONTENT"->>'COMPANY_ID')::int4 AS company_id,
    ("CONTENT"->>'LEAD_ID')::int2 AS lead_id,
    ("CONTENT"->>'TYPE_ID')::varchar AS type_id,
    
    -- Status and activity
    ("CONTENT"->>'EXPORT')::bool AS export,
    ("CONTENT"->>'OPENED')::bool AS opened,
    
    -- Dates and timestamps
    NULLIF("CONTENT"->>'DATE_CREATE','')::timestamptz AS date_create,
    NULLIF("CONTENT"->>'DATE_MODIFY','')::timestamptz AS date_modify,
    NULLIF("CONTENT"->>'LAST_ACTIVITY_TIME','')::timestamptz AS last_activity_time,
    
    -- User references
    ("CONTENT"->>'CREATED_BY_ID')::int2 AS created_by_id,
    ("CONTENT"->>'MODIFY_BY_ID')::int2 AS modify_by_id,
    ("CONTENT"->>'ASSIGNED_BY_ID')::int2 AS assigned_by_id,
    ("CONTENT"->>'LAST_ACTIVITY_BY')::int2 AS last_activity_by,
    
    -- Source and origin
    ("CONTENT"->>'SOURCE_ID')::varchar AS source_id,
    ("CONTENT"->>'SOURCE_DESCRIPTION') AS source_description,
    ("CONTENT"->>'ORIGIN_ID')::int2 AS origin_id,
    ("CONTENT"->>'ORIGINATOR_ID')::varchar AS originator_id,
    ("CONTENT"->>'ORIGIN_VERSION') AS origin_version,
    
    -- UTM parameters
    ("CONTENT"->>'UTM_SOURCE')::varchar AS utm_source,
    ("CONTENT"->>'UTM_MEDIUM')::varchar AS utm_medium,
    ("CONTENT"->>'UTM_CAMPAIGN')::varchar AS utm_campaign,
    ("CONTENT"->>'UTM_CONTENT')::varchar AS utm_content,
    ("CONTENT"->>'UTM_TERM')::varchar AS utm_term,
    
    -- Additional information
    ("CONTENT"->>'COMMENTS')::text AS comments,
    
    -- Custom fields (UF_CRM)
    NULLIF("CONTENT"->>'UF_CRM_VK_WZ','')::bool AS uf_crm_vk_wz,
    ("CONTENT"->>'UF_CRM_AVITO_WZ') AS uf_crm_avito_wz,
    NULLIF("CONTENT"->>'UF_CRM_INSTAGRAM_WZ','false') AS uf_crm_instagram_wz,
    NULLIF("CONTENT"->>'UF_CRM_TELEGRAMID_WZ','false') AS uf_crm_telegramid_wz,
    NULLIF("CONTENT"->>'UF_CRM_TELEGRAMUSERNAME_WZ','false') AS uf_crm_telegramusername_wz,
    NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942124895','false')::jsonb AS uf_crm_contact_1724942124895,
    NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942182263','false')::jsonb AS uf_crm_contact_1724942182263,
    NULLIF(NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942265895','false'),'')::int2 AS uf_crm_contact_1724942265895,
    NULLIF(NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942380589','false'),'')::int2 AS uf_crm_contact_1724942380589,
    NULLIF(NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942491293','false'),'')::int2 AS uf_crm_contact_1724942491293,
    NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942591655','false')::varchar AS uf_crm_contact_1724942591655,
    NULLIF(NULLIF("CONTENT"->>'UF_CRM_CONTACT_1724942632826','false'),'')::int2 AS uf_crm_contact_1724942632826,
    NULLIF("CONTENT"->>'UF_CRM_CONTACT_1725540305021','false')::varchar AS uf_crm_contact_1725540305021
FROM {{source('bitrix', 'btx_raw_contact')}}
WHERE "SUCCESS" = true