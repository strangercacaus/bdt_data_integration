SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_goal')::integer AS id_goal,
("CONTENT"->>'category_ids')::integer[] AS category_ids,
("CONTENT"->>'subcategory_ids')::integer[] AS subcategory_ids,
("CONTENT"->>'product_ids')::integer[] AS product_ids,
("CONTENT"->>'characteristic_ids')::integer[] AS characteristic_ids,
("CONTENT"->>'characteristic_value_ids')::integer[] AS characteristic_value_ids,
("CONTENT"->>'collection_ids')::integer[] AS collection_ids,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification
FROM {{source('bendito','bdt_raw_goals_product_rules')}}