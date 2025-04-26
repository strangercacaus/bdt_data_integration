SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'name')::character varying AS name,
("CONTENT"->>'entity')::integer AS entity,
("CONTENT"->>'order')::integer AS order,
("CONTENT"->>'customer_id')::integer AS customer_id
FROM {{source('bendito','bdt_raw_custom_group')}}