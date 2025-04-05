
  
    

  create  table "bendito_intelligence_dev"."bitrix"."btx_load_errors__dbt_tmp"
  
  
    as
  
  (
    

select 
    'btx_raw_activity' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_activity"
where "SUCCESS" = FALSE

union all

select
    'btx_raw_catalog' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_catalog"
where "SUCCESS" = FALSE

union all

select
    'btx_raw_company' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_company"
where "SUCCESS" = FALSE

union all

select
    'btx_raw_company_userfield' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_company_userfield"
where "SUCCESS" = FALSE

union all

select
    'btx_raw_contact' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_contact"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_currency' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_currency"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_deal' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_deal"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_deal_recurring' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_deal_recurring"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_deal_userfield' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_deal_userfield"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_dealcategory' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_dealcategory"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_dealcategory_stage' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_dealcategory_stage"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_enum_activitydirection' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_enum_activitydirection"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_enum_activitynotifytype' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_enum_activitynotifytype"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_enum_activitypriority' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_enum_activitypriority"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_enum_addresstype' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_enum_addresstype"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_enum_ownertype' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_enum_ownertype"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_lead' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_lead"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_product' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_product"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_status' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_status"
where "SUCCESS" = FALSE

union all

select 
    'btx_raw_user' as data_source,
    now() as checked_at,
    "ID" as id,
    "CONTENT" ->> 'ERROR' as mensagem,
    "CONTENT" ->> 'URL' as url,
    "CONTENT" ->> 'STATUS_CODE' AS status_code
from "bendito_intelligence_dev"."bitrix"."btx_raw_user"
where "SUCCESS" = FALSE
  );
  