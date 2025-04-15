
  
    

  create  table "bendito_intelligence"."default_schema"."bdt_curated_invoice__dbt_tmp"
  
  
    as
  
  (
    
SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'type')::integer AS type,
("CONTENT"->>'nature')::integer AS nature,
("CONTENT"->>'status')::integer AS status,
("CONTENT"->>'delivery_forecast')::timestamp without time zone AS delivery_forecast,
("CONTENT"->>'deadline')::integer AS deadline,
("CONTENT"->>'client')::integer AS client,
("CONTENT"->>'payment_method')::integer AS payment_method,
("CONTENT"->>'value')::numeric AS value,
("CONTENT"->>'tax')::numeric AS tax,
("CONTENT"->>'freight')::numeric AS freight,
("CONTENT"->>'discount')::numeric AS discount,
("CONTENT"->>'total_value')::numeric AS total_value,
("CONTENT"->>'advance')::numeric AS advance,
("CONTENT"->>'automatic_freight')::boolean AS automatic_freight,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'notes')::text AS notes,
("CONTENT"->>'automatic_discount')::boolean AS automatic_discount,
("CONTENT"->>'discount_percentage')::numeric AS discount_percentage,
("CONTENT"->>'price')::integer AS price,
("CONTENT"->>'market')::integer AS market,
("CONTENT"->>'asaas_paymentid')::character varying AS asaas_paymentid,
("CONTENT"->>'asaas_bankslipurl')::character varying AS asaas_bankslipurl,
("CONTENT"->>'payment_status')::integer AS payment_status,
("CONTENT"->>'integration_code')::character varying AS integration_code,
("CONTENT"->>'customer_code')::character varying AS customer_code,
("CONTENT"->>'id_buyer_creation')::integer AS id_buyer_creation,
("CONTENT"->>'id_buyer_modification')::integer AS id_buyer_modification,
("CONTENT"->>'warehouse_id')::integer AS warehouse_id,
("CONTENT"->>'manual_discount')::boolean AS manual_discount,
("CONTENT"->>'id_user_seller')::integer AS id_user_seller,
("CONTENT"->>'addition')::numeric AS addition,
("CONTENT"->>'presenca_comprador')::integer AS presenca_comprador,
("CONTENT"->>'freight_type')::integer AS freight_type,
("CONTENT"->>'invoice_images')::text[] AS invoice_images,
("CONTENT"->>'carrier')::integer AS carrier,
("CONTENT"->>'volume_quantity')::integer AS volume_quantity,
("CONTENT"->>'volume_kind')::character varying AS volume_kind,
("CONTENT"->>'volume_mark')::character varying AS volume_mark,
("CONTENT"->>'volume_numbering')::character varying AS volume_numbering,
("CONTENT"->>'gross_weight')::numeric AS gross_weight,
("CONTENT"->>'net_weight')::numeric AS net_weight,
("CONTENT"->>'business_unit')::integer AS business_unit,
("CONTENT"->>'tags')::text[] AS tags,
("CONTENT"->>'custom_code')::character varying AS custom_code,
("CONTENT"->>'freight_cost_by_weight')::numeric AS freight_cost_by_weight,
("CONTENT"->>'invoice_files')::text[] AS invoice_files,
("CONTENT"->>'addition_percentage')::numeric AS addition_percentage,
("CONTENT"->>'politics_discount')::numeric AS politics_discount,
("CONTENT"->>'politics_addition')::numeric AS politics_addition,
("CONTENT"->>'status_code_integrator')::character varying AS status_code_integrator,
("CONTENT"->>'status_integrator')::character varying AS status_integrator,
("CONTENT"->>'nf_notes')::text AS nf_notes,
("CONTENT"->>'creation_source')::integer AS creation_source,
("CONTENT"->>'tax_ipi')::numeric AS tax_ipi,
("CONTENT"->>'tax_st')::numeric AS tax_st
FROM "bendito_intelligence"."bendito"."bdt_raw_invoice"
  );
  