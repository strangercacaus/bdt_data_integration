select 
	"ID" as page_id,
	("CONTENT" -> 'parent') as parent,
    ("CONTENT" -> 'created_by') as created_by,
    ("CONTENT" -> 'last_edited_by') as last_edited_by,
    ("CONTENT" ->> 'created_time')::timestamptz as created_time,
	("CONTENT" ->> 'last_edited_time')::timestamptz as last_edited_time,
	("CONTENT" -> 'icon') as emoji,
	("CONTENT" ->> 'url')::varchar as url,
    ("CONTENT" ->> 'cover')::varchar as cover,
	("CONTENT" ->> 'object')::varchar as object,
    ("CONTENT" ->> 'archived')::bool as archived,
    ("CONTENT" ->> 'in_trash')::bool as in_trash,
	("CONTENT" ->> 'public_url')::varchar as public_url,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'BeV%7C'
        LIMIT 1
    ) as atualizado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'TfpM'
        LIMIT 1
    ) as verificacao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'VNmI'
        LIMIT 1
    ) as usado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'aNvc'
        LIMIT 1
    ) as tipo,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'bc%3EN'
        LIMIT 1
    ) as secao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'gHQ%3E'
        LIMIT 1
    ) as ultima_edicao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'lZBR'
        LIMIT 1
    ) as status,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'q%7Dmf'
        LIMIT 1
    ) as subsecao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'title'
        LIMIT 1
    ) as pagina,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'notion%3A%2F%2Fwiki%2Fowner_property'
        LIMIT 1
    ) as proprietario
from {{ source('notion', 'ntn_raw_bendito_blueprint') }}
WHERE "SUCCESS" = TRUE