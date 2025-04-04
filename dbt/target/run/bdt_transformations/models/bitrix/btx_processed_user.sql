
  create view "bendito_intelligence_dev"."bitrix"."btx_processed_user__dbt_tmp"
    
    
  as (
    

select "ID"::int2 as id,
	  ("CONTENT"->>'NAME')::varchar as name,
	  ("CONTENT"->>'LAST_NAME')::varchar as last_name,
	  ("CONTENT"->>'EMAIL')::varchar as email,
	  ("CONTENT"->>'ACTIVE')::bool as active,
	  ("CONTENT"->>'XML_ID')::varchar as xml_id,
	  ("CONTENT"->>'IS_ONLINE')::bool as is_online,
	  ("CONTENT"->>'TIME_ZONE')::varchar as time_zone,
	  ("CONTENT"->>'USER_TYPE')::varchar as user_type,
	  ("CONTENT"->>'LAST_LOGIN')::timestamptz as last_login,
	  ("CONTENT"->>'WORK_PHONE')::varchar as work_phone,
	  ("CONTENT"->>'TIMESTAMP_X')::jsonb as timestamp_x,
	  ("CONTENT"->>'PERSONAL_WWW')::varchar as personal_www,
	  ("CONTENT"->>'DATE_REGISTER')::timestamptz as date_register,
	  ("CONTENT"->>'PERSONAL_CITY')::varchar as personal_city,
	  ("CONTENT"->>'UF_DEPARTMENT')::json as uf_department,
	  ("CONTENT"->>'WORK_POSITION')::varchar as work_position,
	  ("CONTENT"->>'PERSONAL_PHOTO')::text as personal_photo,
	  ("CONTENT"->>'UF_PHONE_INNER')::int2 as uf_phone_number,
	  ("CONTENT"->>'PERSONAL_GENDER')::varchar as personal_gender,
	  ("CONTENT"->>'PERSONAL_MOBILE')::varchar as personal_mobile,
	  ("CONTENT"->>'TIME_ZONE_OFFSET')::int4 as time_zone_offset,
	  ("CONTENT"->>'PERSONAL_BIRTHDAY')::varchar as personal_birthday,
	  ("CONTENT"->>'LAST_ACTIVITY_DATE')::jsonb as last_activity_date,
	  ("CONTENT"->>'UF_EMPLOYMENT_DATE')::varchar as uf_employment_date
from "bendito_intelligence_dev"."bitrix"."btx_raw_user"
WHERE "SUCCESS" = true
  );