
  create view "bendito_intelligence_dev"."bitrix"."btx_processed_user__dbt_tmp"
    
    
  as (
    
SELECT
	"ID" :: int2 AS id,
	("CONTENT" ->> 'NAME') :: varchar AS name,
	("CONTENT" ->> 'LAST_NAME') :: varchar AS last_name,
	("CONTENT" ->> 'EMAIL') :: varchar AS email,
	NULLIF(REPLACE("CONTENT" ->> 'ACTIVE','[]',''), '[]') :: bool AS active,
	("CONTENT" ->> 'XML_ID') :: varchar AS xml_id,
	NULLIF(REPLACE("CONTENT" ->> 'IS_ONLINE','[]',''), '[]') :: bool AS is_online,
	("CONTENT" ->> 'TIME_ZONE') :: varchar AS time_zone,
	("CONTENT" ->> 'USER_TYPE') :: varchar AS user_type,
	NULLIF("CONTENT" ->> 'LAST_LOGIN', '') :: timestamptz AS last_login,
	("CONTENT" ->> 'WORK_PHONE') :: varchar AS work_phone,
	NULLIF("CONTENT" ->> 'TIMESTAMP_X', '') :: jsonb AS timestamp_x,
	("CONTENT" ->> 'PERSONAL_WWW') :: varchar AS personal_www,
	NULLIF("CONTENT" ->> 'DATE_REGISTER', '') :: timestamptz AS date_register,
	("CONTENT" ->> 'PERSONAL_CITY') :: varchar AS personal_city,
	NULLIF("CONTENT" ->> 'UF_DEPARTMENT', '') :: json AS uf_department,
	("CONTENT" ->> 'WORK_POSITION') :: varchar AS work_position,
	("CONTENT" ->> 'PERSONAL_PHOTO') :: text AS personal_photo,
	NULLIF("CONTENT" ->> 'UF_PHONE_INNER', '') :: int2 AS uf_phone_number,
	("CONTENT" ->> 'PERSONAL_GENDER') :: varchar AS personal_gender,
	("CONTENT" ->> 'PERSONAL_MOBILE') :: varchar AS personal_mobile,
	NULLIF("CONTENT" ->> 'TIME_ZONE_OFFSET','') :: int4 AS time_zone_offset,
	("CONTENT" ->> 'PERSONAL_BIRTHDAY') :: varchar AS personal_birthday,
	NULLIF("CONTENT" ->> 'LAST_ACTIVITY_DATE','') :: jsonb AS last_activity_date,
	("CONTENT" ->> 'UF_EMPLOYMENT_DATE') :: varchar AS uf_employment_date
from
	"bendito_intelligence_dev"."bitrix"."btx_raw_user"
WHERE
	"SUCCESS" = true
  );