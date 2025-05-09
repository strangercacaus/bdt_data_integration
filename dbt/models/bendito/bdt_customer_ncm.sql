SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'customer')::integer AS customer,
("CONTENT"->>'ncm')::integer AS ncm,
("CONTENT"->>'ipi')::numeric AS ipi,
("CONTENT"->>'pis')::numeric AS pis,
("CONTENT"->>'cofins')::numeric AS cofins,
("CONTENT"->>'icms_operacao')::integer AS icms_operacao,
("CONTENT"->>'cofins_operacao')::integer AS cofins_operacao,
("CONTENT"->>'pis_operacao')::integer AS pis_operacao,
("CONTENT"->>'ipi_operacao')::integer AS ipi_operacao,
("CONTENT"->>'ipi_codigo_enquadramento_legal')::integer AS ipi_codigo_enquadramento_legal,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'categories')::integer[] AS categories,
("CONTENT"->>'products')::integer[] AS products,
("CONTENT"->>'operation_nature')::integer[] AS operation_nature,
("CONTENT"->>'additional_information')::text AS additional_information,
("CONTENT"->>'additional_tax_information')::text AS additional_tax_information
FROM {{source('bendito','bdt_raw_customer_ncm')}}