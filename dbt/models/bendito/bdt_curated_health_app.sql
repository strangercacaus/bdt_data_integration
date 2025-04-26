SELECT 
    -- Primary Key
    ("CONTENT"->>'id')::integer AS id,
    
    -- Application Information
    ("CONTENT"->>'app')::character varying AS app,
    ("CONTENT"->>'version')::character varying AS version,
    
    -- Status Information
    ("CONTENT"->>'up')::boolean AS up,
    
    -- Timestamps
    ("CONTENT"->>'updated_at')::timestamp without time zone AS updated_at
FROM {{source('bendito','bdt_raw_health_app')}}