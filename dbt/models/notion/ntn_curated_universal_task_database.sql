{{ config(
    materialized = 'table',
    post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ],
)}}
select
	page_id as page_id,
	emoji ->> 'emoji' as emoji,
	created_by ->> 'id' as created_by,
	last_edited_by ->> 'id' as last_edited_by,
	created_time as created_time,
	last_edited_time as last_edited_time,
	url as url,
	cover as cover,
	"object" as "object",
	parent ->> 'database_id' as parent,
    archived as archived,
	in_trash as in_trash,
	public_url as public_url,
	(tempo_decorrido -> 'formula' -> 'number' )::int as tempo_decorrido,
	(SELECT 
        COALESCE(
            jsonb_agg(solicitante_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(solicitante->'people') = 'array' 
                THEN solicitante->'people'
                ELSE '[]'::jsonb
            END
        ) AS solicitante_item
    ) as solicitante,
	(peso ->> 'number')::int2 as peso,
	(SELECT 
        COALESCE(
            jsonb_agg(clientes_teste_item->'formula'->>'string'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(clientes_teste->'rollup'->'array') = 'array' 
                THEN clientes_teste->'rollup'->'array'
                ELSE '[]'::jsonb
            END
        ) AS clientes_teste_item
    ) as clientes_teste,
	sla -> 'formula' ->> 'string' as sla,
	release_appcenter ->> 'rich_text' as release_appcenter,
	(SELECT 
        COALESCE(
            jsonb_agg(sprint_number->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(sprint->'multi_select') = 'array' 
                THEN sprint->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS sprint_number
    ) as sprint,
	(entrou_em_a_testar_em -> 'date' ->> 'start')::timestamptz as entrou_em_a_testar_em,
	(SELECT 
        COALESCE(
            jsonb_agg(customer->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(customer_id->'multi_select') = 'array' 
                THEN solicitante->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS customer
    ) as customer_id,
	status -> 'select' ->> 'name' as status,
	(etapa_atualizada_em -> 'date' ->> 'start')::timestamptz as etapa_atualizada_em,
	area -> 'select' ->>'name' as area,
	(SELECT 
        COALESCE(
            jsonb_agg(branch_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(branch->'multi_select') = 'array' 
                THEN branch->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS branch_item
    ) as branch,
	(SELECT 
        COALESCE(
            jsonb_agg(responsavel_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(responsavel->'people') = 'array' 
                THEN responsavel->'people'
                ELSE '[]'::jsonb
            END
        ) AS responsavel_item
    ) as responsavel,
	atualizado_por -> 'last_edited_by' ->> 'id' as atualizado_por,
	prioridade -> 'select' -> 'name' as prioridade,
	(atualizado_em ->> 'last_edited_time')::timestamptz as atualizado_em,
	entrou_em_finalizado_em -> 'date' ->> 'start' as entrou_em_finalizado_em,
	(SELECT 
        COALESCE(
            jsonb_agg(bloqueando_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(bloqueando->'relation') = 'array' 
                THEN bloqueando->'people'
                ELSE '[]'::jsonb
            END
        ) AS bloqueando_item
    ) as bloqueando,
	link_externo ->> 'url' as link_externo,
	(SELECT 
        COALESCE(
            jsonb_agg(modulo_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(modulo_do_sistema->'multi_select') = 'array' 
                THEN modulo_do_sistema->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS modulo_item
    )  as modulo_do_sistema,
	(entrou_em_testando_em -> 'date' ->> 'start')::timestamptz as entrou_em_testando_em,
	justificativa -> 'select' ->> 'name' as justificativa,
	(dias_ate_o_sla -> 'formula' ->> 'number')::int2 as dias_ate_o_sla,
	(SELECT 
        COALESCE(
            jsonb_agg(projetos_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(projetos->'relation') = 'array' 
                THEN projetos->'relation'
                ELSE '[]'::jsonb
            END
        ) AS projetos_item
    ) as projetos,
	(SELECT 
        COALESCE(
            jsonb_agg(nome_do_cliente_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(nome_do_cliente->'multi_select') = 'array' 
                THEN nome_do_cliente->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS nome_do_cliente_item
    ) as nome_do_cliente,
	(entrou_em_a_fazer_em -> 'date' ->> 'start')::timestamptz as entrou_em_a_fazer_em,
	entered_correction_at -> 'date' -> 'start' as entered_correction_at,
	(SELECT 
        COALESCE(
            jsonb_agg(item_filho_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(item_filho->'relation') = 'array' 
                THEN item_filho->'relation'
                ELSE '[]'::jsonb
            END
        ) AS item_filho_item
    ) as item_filho,
	id_da_tarefa -> 'formula' ->> 'string' as id_da_tarefa,
	inicio_do_sla -> 'date' ->> 'start' as inicio_do_sla,
	(dias_de_desenvolvimento -> 'formula' ->> 'number')::int2 as dias_de_desenvolvimento,
	(integrado -> 'checkbox')::bool as integrado,
	(entrou_em_aprovado_em -> 'date' ->> 'start')::timestamptz as entrou_em_aprovado_em,
	(entrou_em_fazendo_em -> 'date' ->> 'start')::timestamptz as entrou_em_fazendo_em,
	(SELECT titulo_item->'text'->'content'
    FROM jsonb_array_elements( titulo->'title') AS titulo_item
    LIMIT 1
    ) as titulo,
	(SELECT 
        COALESCE(
            jsonb_agg(melhoria_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(melhoria->'relation') = 'array' 
                THEN melhoria->'relation'
                ELSE '[]'::jsonb
            END
        ) AS melhoria_item
    ) as melhoria,
	(entrou_em_aguardando_versao_em -> 'date' ->> 'start')::timestamptz as entrou_em_aguardando_versao_em,
	(atraso ->> 'number')::int2 as atraso,
	(SELECT 
        COALESCE(
            jsonb_agg(integracao_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(integracao->'multi_select') = 'array' 
                THEN integracao->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS integracao_item
    ) as integracao,
	(dias_ate_a_conclusao -> 'formula' ->> 'number')::int2 as dias_ate_a_conclusao,
	(dias_ate_o_desenvolvimento -> 'formula' ->> 'number')::int2 as dias_ate_o_desenvolvimento,
	--id_dos_clientes_teste as id_dos_clientes_teste,
	etapa -> 'select' ->> 'name' as etapa,
	(prazo -> 'date' ->> 'start'):: timestamptz as prazo,
	(SELECT 
        COALESCE(
            jsonb_agg(ambiente_item->>'name'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(ambiente->'multi_select') = 'array' 
                THEN ambiente->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS ambiente_item
    ) as ambiente,
	criado_por -> 'created_by' ->> 'id' as criado_por,
	(SELECT 
        COALESCE(
            jsonb_agg(item_pai_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(item_pai->'multi_select') = 'array' 
                THEN item_pai->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS item_pai_item
    ) as item_pai,
	(criado_em ->> 'created_time')::timestamptz as criado_em,
	(SELECT 
        COALESCE(
            jsonb_agg(bloqueado_por_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(bloqueado_por->'multi_select') = 'array' 
                THEN bloqueado_por->'multi_select'
                ELSE '[]'::jsonb
            END
        ) AS bloqueado_por_item
    ) as bloqueado_por,
	array[ (duracao_da_tarefa -> 'formula' -> 'date' ->>'start')::timestamptz, (duracao_da_tarefa -> 'formula' -> 'date' ->>'end')::timestamptz ] as duracao_da_tarefa,
	(dias_na_etapa -> 'formula' ->> 'number')::int2 as dias_na_etapa,
	(entrou_em_backlog_em -> 'date' ->> 'start')::timestamptz as entrou_em_backlog_em,
	okr-> 'select' ->> 'name' as okr,
	(id_da_pagina -> 'unique_id' ->> 'number')::int2 as id_da_pagina,
	(SELECT 
        COALESCE(
            jsonb_agg(tester_item->>'id'),
            '[]'::jsonb
        )
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(tester->'people') = 'array' 
                THEN tester->'people'
                ELSE '[]'::jsonb
            END
        ) AS tester_item
    ) as tester
from {{ ref('ntn_processed_universal_task_database') }}

