
  
    

  create  table "bendito_intelligence"."bendito"."bdt_curated_client__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'resale')::boolean AS resale,
("CONTENT"->>'notes')::text AS notes,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'person')::integer AS person,
("CONTENT"->>'id_client')::integer AS id_client,
("CONTENT"->>'asaas_customerid')::character varying AS asaas_customerid,
("CONTENT"->>'customer_code')::character varying AS customer_code,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'credit_limit')::numeric AS credit_limit,
("CONTENT"->>'block_sale')::boolean AS block_sale,
("CONTENT"->>'pricing_type')::integer AS pricing_type,
("CONTENT"->>'tags')::text AS tags,
("CONTENT"->>'payment_method')::integer AS payment_method,
("CONTENT"->>'company_name')::character varying AS company_name,
("CONTENT"->>'creation_source')::integer AS creation_source,
("CONTENT"->>'preferred_deadline')::integer AS preferred_deadline,
("CONTENT"->>'calculate_ipi')::boolean AS calculate_ipi,
("CONTENT"->>'available_credit_limit')::numeric AS available_credit_limit,
("CONTENT"->>'minimum_order')::numeric AS minimum_order,
("CONTENT"->>'ecommerce_minimum_order')::numeric AS ecommerce_minimum_order,
("CONTENT"->>'minimum_quantity')::integer AS minimum_quantity
FROM "bendito_intelligence"."bendito"."bdt_raw_client"
  );
  