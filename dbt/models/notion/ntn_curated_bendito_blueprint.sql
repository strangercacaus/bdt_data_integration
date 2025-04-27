{{ config(
    post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ]
)}}
select
	page_id as page_id,
	created_by ->> 'id' as created_by,
	last_edited_by ->> 'id' as last_edited_by,
	created_time as created_time,
	last_edited_time as last_edited_time,
	emoji -> 'external' ->> 'url' as emoji,
	url as url,
	cover as cover,
    object as object,
	parent ->> 'database_id' as parent,
    archived as archived,
	in_trash as in_trash,
	public_url as public_url,
    (pagina -> 'title' -> 0 -> 'text' ->> 'content') as pagina,
    (status -> 'status' ->> 'name') as status,
    (atualizado_em -> 'date' ->> 'start')::timestamptz as atualizado_em,
    (ultima_edicao -> 'last_edited_time' ->> 'start')::timestamptz as ultima_edicao_em,
	(verificacao -> 'verification' ->> 'state' ) as estado_verificacao,
    (verificacao -> 'verification' -> 'date' ->> 'start')::timestamptz as verificacao_em,
    (verificacao -> 'verification' -> 'date' ->> 'end')::timestamptz as verificacao_ate,
    (verificacao -> 'verification' ->> 'verified_by' ) as verificado_por,
    (SELECT 
        COALESCE(
            jsonb_agg(item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(usado_em->'multi_select') = 'array' 
                THEN usado_em->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS item
    ) as usado_em,
    (tipo -> 'select' ->> 'name') as tipo,
    (secao -> 'select' ->> 'name') as secao,
    (subsecao -> 'select' ->> 'name') as subsecao,
    (proprietario -> 'select' ->> 'name') as proprietario
from {{ ref('ntn_processed_bendito_blueprint') }}