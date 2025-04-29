select
    -- Primary identifiers
    ("CONTENT"->>'ID')::int2 AS id,
    ("CONTENT"->>'TITLE') AS title,
    ("CONTENT"->>'NAME') AS name,
    ("CONTENT"->>'LAST_NAME') AS last_name,
    ("CONTENT"->>'SECOND_NAME') AS second_name,
    ("CONTENT"->>'HONORIFIC') AS honorific,
    ("CONTENT"->>'POST') AS post,
    ("CONTENT"->>'BIRTHDATE')::varchar AS birthdate,
    
    -- Contact information
    ("CONTENT"->'EMAIL') AS email,
    ("CONTENT"->'PHONE') AS phone,
    ("CONTENT"->'IM') AS im,
    ("CONTENT"->>'HAS_EMAIL') AS has_email,
    ("CONTENT"->>'HAS_PHONE')::bool AS has_phone,
    ("CONTENT"->>'HAS_IMOL') AS has_imol,
    
    -- Address information
    ("CONTENT"->>'ADDRESS') AS address,
    ("CONTENT"->>'ADDRESS_2') AS address_2,
    ("CONTENT"->>'ADDRESS_CITY') AS address_city,
    ("CONTENT"->>'ADDRESS_COUNTRY') AS address_country,
    ("CONTENT"->>'ADDRESS_COUNTRY_CODE') AS address_country_code,
    ("CONTENT"->>'ADDRESS_LOC_ADDR_ID') AS address_loc_addr_id,
    ("CONTENT"->>'ADDRESS_POSTAL_CODE') AS address_postal_code,
    ("CONTENT"->>'ADDRESS_PROVINCE') AS address_province,
    ("CONTENT"->>'ADDRESS_REGION') AS address_region,
    
    -- Company information
    NULLIF("CONTENT"->>'COMPANY_ID','')::int2 AS company_id,
    ("CONTENT"->>'COMPANY_TITLE') AS company_title,
    ("CONTENT"->>'CURRENCY_ID') AS currency_id,
    
    -- Status and activity
    ("CONTENT"->>'STATUS_ID') AS status_id,
    ("CONTENT"->>'STATUS_DESCRIPTION') AS status_description,
    ("CONTENT"->>'STATUS_SEMANTIC_ID')::varchar AS status_semantic_id,
    ("CONTENT"->>'OPENED')::bool AS opened,
    ("CONTENT"->>'IS_MANUAL_OPPORTUNITY')::bool AS is_manual_opportunity,
    ("CONTENT"->>'IS_RETURN_CUSTOMER')::bool AS is_return_customer,
    ("CONTENT"->>'OPPORTUNITY')::numeric AS opportunity,
    
    -- Dates and timestamps
    NULLIF("CONTENT"->>'DATE_CREATE','')::timestamptz AS date_create,
    NULLIF("CONTENT"->>'DATE_MODIFY','') AS date_modify,
    NULLIF("CONTENT"->>'DATE_CLOSED','') AS date_closed,
    NULLIF("CONTENT"->>'LAST_ACTIVITY_TIME','')::timestamptz AS last_activity_time,
    NULLIF("CONTENT"->>'MOVED_TIME','')::timestamptz AS moved_time,
    
    -- User references
    NULLIF("CONTENT"->>'CREATED_BY_ID','')::int2 AS created_by_id,
    NULLIF("CONTENT"->>'MODIFY_BY_ID','')::int2 AS modify_by_id,
    NULLIF("CONTENT"->>'ASSIGNED_BY_ID','')::int2 AS assigned_by_id,
    NULLIF("CONTENT"->>'LAST_ACTIVITY_BY','')::int2 AS last_activity_by,
    NULLIF("CONTENT"->>'MOVED_BY_ID','')::int2 AS moved_by_id,
    NULLIF("CONTENT"->>'CONTACT_ID','')::int4 AS contact_id,
    
    -- Source and origin
    ("CONTENT"->>'SOURCE_ID') AS source_id,
    ("CONTENT"->>'SOURCE_DESCRIPTION') AS source_description,
    ("CONTENT"->>'ORIGIN_ID')::int2 AS origin_id,
    ("CONTENT"->>'ORIGINATOR_ID') AS originator_id,
    
    -- UTM parameters
    ("CONTENT"->>'UTM_SOURCE') AS utm_source,
    ("CONTENT"->>'UTM_MEDIUM') AS utm_medium,
    ("CONTENT"->>'UTM_CAMPAIGN') AS utm_campaign,
    ("CONTENT"->>'UTM_CONTENT') AS utm_content,
    ("CONTENT"->>'UTM_TERM') AS utm_term,
    
    -- Social media and additional fields
    ("CONTENT"->>'UF_CRM_VK_WZ') AS uf_crm_vk_wz,
    ("CONTENT"->>'UF_CRM_AVITO_WZ') AS uf_crm_avito_wz,
    ("CONTENT"->>'UF_CRM_INSTAGRAM_WZ') AS uf_crm_instagram_wz,
    ("CONTENT"->>'UF_CRM_TELEGRAMID_WZ') AS uf_crm_telegramid_wz,
    ("CONTENT"->>'UF_CRM_TELEGRAMUSERNAME_WZ') AS uf_crm_telegramusername_wz,
    
    -- Additional fields
    ("CONTENT"->>'COMMENTS') AS comments,
    ("CONTENT"-> 'LINK') AS link
FROM {{ source('bitrix', 'btx_raw_crm_lead') }}