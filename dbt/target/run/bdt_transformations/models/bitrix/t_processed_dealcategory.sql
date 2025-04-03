
  
    

  create  table "bendito_intelligence_dev"."transformed_transformed"."t_processed_dealcategory__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
  "ID" as id,
  ("CONTENT"->>'NAME')::varchar as name,
  ("CONTENT"->>'SORT')::int4 as sort,
  ("CONTENT"->>'CREATED_DATE')::timestamp as created_date
FROM bitrix.t_raw_dealcategory
  );
  