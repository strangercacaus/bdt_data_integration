SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_invoice')::integer AS id_invoice,
("CONTENT"->>'id_approver')::integer AS id_approver,
("CONTENT"->>'id_invoice_workflow')::integer AS id_invoice_workflow,
("CONTENT"->>'vote')::integer AS vote,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'comments')::text AS comments
FROM {{source('bendito','bdt_raw_invoice_approvers')}}