SELECT
	-- Primary identifiers
	"ID"::int2 AS id,
	("CONTENT"->>'NAME')::varchar AS name,
	("CONTENT"->>'LAST_NAME')::varchar AS last_name,
	("CONTENT"->>'XML_ID')::varchar AS xml_id,
	("CONTENT"->>'USER_TYPE')::varchar AS user_type,
	
	-- Contact information
	("CONTENT"->>'EMAIL')::varchar AS email,
	("CONTENT"->>'WORK_PHONE')::varchar AS work_phone,
	("CONTENT"->>'PERSONAL_MOBILE')::varchar AS personal_mobile,
	NULLIF("CONTENT"->>'UF_PHONE_INNER', '')::int2 AS uf_phone_number,
	
	-- Personal information
	("CONTENT"->>'PERSONAL_GENDER')::varchar AS personal_gender,
	("CONTENT"->>'PERSONAL_BIRTHDAY')::varchar AS personal_birthday,
	("CONTENT"->>'PERSONAL_CITY')::varchar AS personal_city,
	("CONTENT"->>'PERSONAL_WWW')::varchar AS personal_www,
	("CONTENT"->>'PERSONAL_PHOTO')::text AS personal_photo,
	
	-- Work information
	("CONTENT"->>'WORK_POSITION')::varchar AS work_position,
	NULLIF("CONTENT"->>'UF_DEPARTMENT', '')::json AS uf_department,
	("CONTENT"->>'UF_EMPLOYMENT_DATE')::varchar AS uf_employment_date,
	
	-- Status and activity
	NULLIF(REPLACE("CONTENT"->>'ACTIVE','[]',''), '[]')::bool AS active,
	NULLIF(REPLACE("CONTENT"->>'IS_ONLINE','[]',''), '[]')::bool AS is_online,
	
	-- Dates and timestamps
	NULLIF("CONTENT"->>'LAST_LOGIN', '')::timestamptz AS last_login,
	NULLIF("CONTENT"->>'TIMESTAMP_X', '')::varchar AS timestamp_x,
	NULLIF("CONTENT"->>'DATE_REGISTER', '')::timestamptz AS date_register,
	NULLIF("CONTENT"->>'LAST_ACTIVITY_DATE','')::jsonb AS last_activity_date,
	
	-- Time zone settings
	("CONTENT"->>'TIME_ZONE')::varchar AS time_zone,
	NULLIF("CONTENT"->>'TIME_ZONE_OFFSET','')::int4 AS time_zone_offset
FROM {{ source('bitrix', 'btx_raw_crm_user') }}
WHERE "SUCCESS" = true