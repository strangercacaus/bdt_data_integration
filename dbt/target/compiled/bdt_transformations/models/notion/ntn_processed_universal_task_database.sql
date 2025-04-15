
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
        WHERE value ->> 'id' = 'cQuD'
        LIMIT 1
    ) as tempo_decorrido,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3EtXq'
        LIMIT 1
    ) as solicitante,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3AnD%7B'
        LIMIT 1
    ) as peso,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'RZok'
        LIMIT 1
    ) as entrou_em_a_fazer_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'W%3EOG'
        LIMIT 1
    ) as clientes_teste,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%5CE%7CT'
        LIMIT 1
    ) as sla,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'bxha'
        LIMIT 1
    ) as release_appcenter,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'DvWV'
        LIMIT 1
    ) as sprint,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'kOr%5C'
        LIMIT 1
    ) as entrou_em_a_testar_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3CUw_'
        LIMIT 1
    ) as customer_id,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'jl%3FW'
        LIMIT 1
    ) as status,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'jqsw'
        LIMIT 1
    ) as etapa_atualizada_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'R%5BbO'
        LIMIT 1
    ) as area,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%60%7CVU'
        LIMIT 1
    ) as branch,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'gmhw'
        LIMIT 1
    ) as responsavel,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%5E%3A%7BY'
        LIMIT 1
    ) as atualizado_por,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'rqJ'''
        LIMIT 1
    ) as prioridade,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'xJBR'
        LIMIT 1
    ) as atualizado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'zr%40c'
        LIMIT 1
    ) as entrou_em_finalizado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'I%5EJJ'
        LIMIT 1
    ) as bloqueando,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'uJTd'
        LIMIT 1
    ) as link_externo,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '_i%7CT'
        LIMIT 1
    ) as modulo_do_sistema,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'z%7DO~'
        LIMIT 1
    ) as entrou_em_testando_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'AM%7D%40'
        LIMIT 1
    ) as justificativa,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'Y%5BHF'
        LIMIT 1
    ) as dias_ate_o_sla,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'rvEg'
        LIMIT 1
    ) as projetos,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'k%7BC%60'
        LIMIT 1
    ) as nome_do_cliente,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'MMDE'
        LIMIT 1
    ) as entered_correction_at,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'BT%5E%7C'
        LIMIT 1
    ) as item_filho,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'ytoL'
        LIMIT 1
    ) as id_da_tarefa,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'tklV'
        LIMIT 1
    ) as inicio_do_sla,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'bY%3Bi'
        LIMIT 1
    ) as dias_de_desenvolvimento,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'ILQ%3B'
        LIMIT 1
    ) as integrado,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'RIPB'
        LIMIT 1
    ) as entrou_em_aprovado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'dT%3BM'
        LIMIT 1
    ) as entrou_em_fazendo_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'title'
        LIMIT 1
    ) as titulo,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'xy%60%7D'
        LIMIT 1
    ) as melhoria,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'isqN'
        LIMIT 1
    ) as entrou_em_aguardando_versao_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'wV%5D%7B'
        LIMIT 1
    ) as atraso,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'JDHn'
        LIMIT 1
    ) as integracao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'j%5D%3DA'
        LIMIT 1
    ) as dias_ate_a_conclusao,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3C_%40%60'
        LIMIT 1
    ) as dias_ate_o_desenvolvimento,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3Ady%60'
        LIMIT 1
    ) as id_dos_clientes_teste,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '3E6J'
        LIMIT 1
    ) as etapa,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'Y%7DF%7C'
        LIMIT 1
    ) as prazo,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%5DQTb'
        LIMIT 1
    ) as ambiente,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%40%60v%3C'
        LIMIT 1
    ) as criado_por,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3El~P'
        LIMIT 1
    ) as item_pai,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'TT%40f'
        LIMIT 1
    ) as criado_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%5BkYo'
        LIMIT 1
    ) as bloqueado_por,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'eWnA'
        LIMIT 1
    ) as duracao_da_tarefa,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = '%3Bn%3ES'
        LIMIT 1
    ) as dias_na_etapa,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'n%3AJ%5D'
        LIMIT 1
    ) as entrou_em_backlog_em,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'NKa%3D'
        LIMIT 1
    ) as okr,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'ooSm'
        LIMIT 1
    ) as id_da_pagina,
    (
        SELECT value
        FROM jsonb_each("CONTENT" -> 'properties')
        WHERE value ->> 'id' = 'gOo%3A'
        LIMIT 1
    ) as tester
from "bendito_intelligence"."notion"."ntn_raw_universal_task_database"
WHERE "SUCCESS" = TRUE