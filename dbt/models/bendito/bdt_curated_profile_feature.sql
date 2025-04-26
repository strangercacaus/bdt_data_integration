SELECT ("CONTENT"->>'id_profile')::integer AS id_profile,
("CONTENT"->>'id_feature')::integer AS id_feature,
("CONTENT"->>'id_user_creation')::integer AS id_user_creation,
("CONTENT"->>'grant_insert')::boolean AS grant_insert,
("CONTENT"->>'grant_update')::boolean AS grant_update,
("CONTENT"->>'grant_delete')::boolean AS grant_delete,
("CONTENT"->>'time_creation')::timestamp without time zone AS time_creation,
("CONTENT"->>'id_user_modification')::integer AS id_user_modification,
("CONTENT"->>'time_modification')::timestamp without time zone AS time_modification,
("CONTENT"->>'id')::integer AS id
FROM {{source('bendito','bdt_raw_profile_feature')}}