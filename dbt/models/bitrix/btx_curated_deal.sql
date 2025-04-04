{ { config(
	materialized = 'table',
	unique_key = 'id',
) } }
SELECT
	x.id,
	x.title AS titulo,
	x1.name AS estagio,
	--x.category_id, -- nenhum deal possui este campo preenchido
	x.opportunity AS mrr,
	x.company_id,
	x.contact_id,
	x2.name AS atribuido_por,
	x.opened AS aberto,
	x.begin_date AS data_de_inicio,
	x.closed AS encerrado,
	x.close_date AS data_de_encerramento,
	x.date_create AS criado_em,
	x5.name AS criado_por,
	x.date_modify AS modificado_em,
	x4.name AS modificdo_por,
	x.moved_time AS movido_em,
	x3.name AS movido_por,
	x.last_activity_time AS ultima_atividade_em,
	x6.name AS ultima_atividade_por,
	--x.is_new, -- provavelmente indica se o deal é recente
	x.lead_id AS id_lead,
	x.type_id AS id_tipo,
	x.comments AS comentarios,
	--x.quote_id, -- provavelmente o id do orçamento
	--x.origin_id, -- nenhum valor preenchido
	x.source_id id_da_fonte,
	--x.source_description descricao_da_fonte, -- não utilizado
	--tax_value, -- valor do imposto
	x.currency_id AS moeda,
	--x.location_id, -- não utilizado
	--x.probability, -- provavelmente probabilidade de fechamento, não utilizado
	x.is_recurring AS recorrente,
	--x.additional_info, -- não utilizado
	--x.stage_semantic_id, -- não utilizado
	--x.originator_id, -- não utilizado
	x.is_manual_opportunity oportunidade_manual,
	x.is_return_customer cliente_retorno,
	x.is_repeated_approach abordagem_repetida,
	x.uf_crm_1718891707656 AS quantidade_de_usuarios,
	x.uf_crm_1718893063318 AS ponto_focal,
	x.uf_crm_1721248408791 AS email_ponto_focal,
	x.uf_crm_1718895613676 AS erp,
	x.uf_crm_1718895632531 AS segmento,
	x.uf_crm_1718895837277 AS perfil,
	x.uf_crm_1719234554494 AS email_financeiro,
	x.uf_crm_1719234611470 AS forma_pgto_implementacao,
	x.uf_crm_1719234653377 AS forma_pgto_recorrencia,
	x.uf_crm_1719851689133 AS vr_negociado_recorrencia,
	x.uf_crm_1719851724735 AS vr_negociado_implementacao,
	x.uf_crm_1719942627227 AS anexar_o_contrato,
	x.uf_crm_1720015348995 AS contrato_do_setup,
	x.uf_crm_1720198477652 AS valor_negociado_ativacao,
	x.uf_crm_1720198502628 AS valor_negociado_integracao,
	x.uf_crm_1721083795801 AS quantidade_usuarios_ativos,
	x.uf_crm_1721083820061 AS vr_transacionado_em_janeiro,
	x.uf_crm_1721083836488 AS vr_transacionado_em_fevereiro,
	x.uf_crm_1721083849804 AS vr_transacionado_em_marco,
	x.uf_crm_1721083861185 AS vr_transacionado_em_abril,
	x.uf_crm_1721083874978 AS vr_transacionado_em_maio,
	x.uf_crm_1721083894608 AS vr_transacionado_em_junho,
	x.uf_crm_1721083906720 AS vr_transacionado_em_julho,
	x.uf_crm_1724264534445 AS vr_transacionado_em_agosto,
	x.uf_crm_1726007497 AS vr_transacionado_em_setembro,
	x.uf_crm_1728416073106 AS vr_transacionado_em_outubro,
	x.uf_crm_1733168617374 AS vr_transacionado_novembro,
	x.uf_crm_1721226126 AS modulo_do_sistema,
	x.uf_crm_1721227124 AS ambiente,
	x.uf_crm_1721241190 AS classificacao_do_atendimento,
	x.uf_crm_1721243220370 AS area,
	x.uf_crm_1721243302036 AS descricao,
	x.uf_crm_1721243367 AS arquivos,
	x.uf_crm_1721244413245 AS carro_chefe,
	x.uf_crm_1721244451568 AS faturamento_medio_mensal,
	x.uf_crm_1721244493157 AS qtd_media_pedidos_mes,
	x.uf_crm_1721244526025 AS vendedor_destaque,
	x.uf_crm_1721244560622 AS pretende_estar_operando_em,
	x.uf_crm_1721244826264 AS motivo_do_cancelamento,
	x.uf_crm_1721244999810 AS o_que_poderia_ter_sido_diferente,
	x.uf_crm_1721248388070 AS tipo_de_aumento_de_receita,
	x.uf_crm_1721406278811 AS data_do_churn,
	x.uf_crm_1724261215 AS media_de_faturamento,
	x.uf_crm_1725574177266 AS id_asaas,
	x.uf_crm_1725574322 AS status_do_pagamento,
	x.uf_crm_1725548469220 AS data_solicitacao_upsell,
	x.uf_crm_1726002322164 AS expectativa_com_o_projeto,
	x.uf_crm_1726003734 AS status_do_projeto,
	x.uf_crm_1729086829554 AS detalhes_motivo_cancelamento,
	x.uf_crm_1729086887055 AS data_solicitacao_cancelamento,
	x.uf_crm_1732567021643 AS como_avalia_implantacao_bendito,
	x.uf_crm_1733491265514 AS novo_valor,
	x.uf_crm_1738931838 AS cliente_ja_possui_base_bendito,
	x.uf_crm_1741275511723 AS quantidade_de_pedidos,
	x.uf_crm_1741275566502 AS valor_faturado,
	x.uf_crm_1741275596050 AS produto,
	x.uf_crm_1741275610428 AS valor_vendido,
	x.uf_crm_1741275634394 AS qtd_vendida,
	--x.uf_crm_66d0a6a12e620, -- não é usado
	--x.uf_crm_66d0a6a146d20, -- não é usado
	--x.uf_crm_66d0a6a1529fd, -- não é usado
	--x.uf_crm_66d0a6a15d94f, -- não é usado
	--x.uf_crm_66d0a6a169edc, -- não é usado
	x.uf_crm_deal_1719236124580 AS plano,
	x.uf_crm_deal_1725540405848 AS tem_desenvolvimento_bendito,
	x.uf_crm_deal_1725540759541 AS tipo_de_desenvolvimento,
	x.uf_crm_deal_1725569571488 AS expectativa_valor_recorrencia,
	x.uf_crm_deal_1725569886689 AS pessoa_responsavel_pelo_lead,
	x.uf_crm_deal_1727374223411 AS qual_a_dor_do_cliente,
	x.uf_crm_deal_1727374263239 AS ja_utilizavam_solucao,
	x.uf_crm_deal_1732567150103 AS conta_pra_gente_porque,
	x.uf_crm_deal_1738069738944 AS tipo_de_produto_que_a_empresa_vende,
	x.uf_crm_deal_1738069820827 AS como_fazem_gestao_de_pedidos_hj,
	x.uf_crm_deal_1722632496832 AS email_da_pessoa_ponto_focal --,
	--x.utm_medium, --não é usado
	--x.utm_source, --não é usado
	--x.utm_term, -- não é usado
	--x.utm_content, -- não é usado
	--x.utm_campaign -- não é usado
FROM
	{ { ref('btx_processed_deal') } } x
	LEFT JOIN { { ref('btx_processed_deal_stage') } } x1 ON x.stage_id = x1.status_id
	LEFT JOIN { { ref('btx_processed_user') } } x2 ON x.assigned_by_id = x2."id"
	LEFT JOIN { { ref('btx_processed_user') } } x3 ON x.moved_by_id = x3."id"
	LEFT JOIN { { ref('btx_processed_user') } } x4 ON x.modify_by_id = x4."id"
	LEFT JOIN { { ref('btx_processed_user') } } x5 ON x.created_by_id = x5."id"
	LEFT JOIN { { ref('btx_processed_user') } } x6 ON x.last_activity_by = x6."id"