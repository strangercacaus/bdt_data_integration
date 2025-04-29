SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'price_table_id')::integer AS price_table_id,
("CONTENT"->>'payment_method_id')::integer AS payment_method_id
FROM {{source('bendito','bdt_raw_payment_method_price_table')}}