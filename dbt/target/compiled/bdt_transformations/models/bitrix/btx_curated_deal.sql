
select
	x.id,
	x.title as titulo,
	x1.name as estagio,
	--x.category_id, -- nenhum deal possui este campo preenchido
	x.opportunity as mrr,
	x.company_id,
	x.contact_id,
	x2.name as atribuido_por,
	x.opened as aberto,
	x.begin_date as data_de_inicio,
	x.closed as encerrado,
	x.close_date as data_de_encerramento,
	x.date_create as criado_em,
	x5.name as criado_por,
	x.date_modify as modificado_em,
	x4.name as modificdo_por,
	x.moved_time as movido_em,
	x3.name as movido_por,
	x.last_activity_time as ultima_atividade_em,
	x6.name as ultima_atividade_por,
	--x.is_new, -- provavelmente indica se o deal é recente
	x.lead_id as id_lead,
	x.type_id as id_tipo,
	x.comments as comentarios,
	--x.quote_id, -- provavelmente o id do orçamento
	--x.origin_id, -- nenhum valor preenchido
	x.source_id id_da_fonte,
	--x.source_description descricao_da_fonte, -- não utilizado
	--tax_value, -- valor do imposto
	x.currency_id as moeda,
	--x.location_id, -- não utilizado
	--x.probability, -- provavelmente probabilidade de fechamento, não utilizado
	x.is_recurring as recorrente,
	--x.additional_info, -- não utilizado
	--x.stage_semantic_id, -- não utilizado
	--x.originator_id, -- não utilizado
	x.is_manual_opportunity oportunidade_manual,
	x.is_return_customer cliente_retorno,
	x.is_repeated_approach abordagem_repetida,
	x.uf_crm_1718891707656 as quantidade_de_usuarios,
	x.uf_crm_1718893063318 as ponto_focal,
	x.uf_crm_1721248408791 as email_ponto_focal,
	x.uf_crm_1718895613676 as erp,
	x.uf_crm_1718895632531 as segmento,
	x.uf_crm_1718895837277 as perfil,
	x.uf_crm_1719234554494 as email_financeiro,
	x.uf_crm_1719234611470 as forma_pgto_implementacao,
	x.uf_crm_1719234653377 as forma_pgto_recorrencia,
	x.uf_crm_1719851689133 as vr_negociado_recorrencia,
	x.uf_crm_1719851724735 as vr_negociado_implementacao,
	x.uf_crm_1719942627227 as anexar_o_contrato,
	x.uf_crm_1720015348995 as contrato_do_setup,
	x.uf_crm_1720198477652 as valor_negociado_ativacao,
	x.uf_crm_1720198502628 as valor_negociado_integracao,
	x.uf_crm_1721083795801 as quantidade_usuarios_ativos,
	x.uf_crm_1721083820061 as vr_transacionado_em_janeiro,
	x.uf_crm_1721083836488 as vr_transacionado_em_fevereiro,
	x.uf_crm_1721083849804 as vr_transacionado_em_marco,
	x.uf_crm_1721083861185 as vr_transacionado_em_abril,
	x.uf_crm_1721083874978 as vr_transacionado_em_maio,
	x.uf_crm_1721083894608 as vr_transacionado_em_junho,
	x.uf_crm_1721083906720 as vr_transacionado_em_julho,
	x.uf_crm_1724264534445 as vr_transacionado_em_agosto,
	x.uf_crm_1726007497 as vr_transacionado_em_setembro,
	x.uf_crm_1728416073106 as vr_transacionado_em_outubro,
	x.uf_crm_1733168617374 as vr_transacionado_novembro,
	x.uf_crm_1721226126 as modulo_do_sistema,
	x.uf_crm_1721227124 as ambiente,
	x.uf_crm_1721241190 as classificacao_do_atendimento,
	x.uf_crm_1721243220370 as area,
	x.uf_crm_1721243302036 as descricao,
	x.uf_crm_1721243367 as arquivos,
	x.uf_crm_1721244413245 as carro_chefe,
	x.uf_crm_1721244451568 as faturamento_medio_mensal,
	x.uf_crm_1721244493157 as qtd_media_pedidos_mes,
	x.uf_crm_1721244526025 as vendedor_destaque,
	x.uf_crm_1721244560622 as pretende_estar_operando_em,
	x.uf_crm_1721244826264 as motivo_do_cancelamento,
	x.uf_crm_1721244999810 as o_que_poderia_ter_sido_diferente,
	x.uf_crm_1721248388070 as tipo_de_aumento_de_receita,
	x.uf_crm_1721406278811 as data_do_churn,
	x.uf_crm_1724261215 as media_de_faturamento,
	x.uf_crm_1725574177266 as id_asaas,
	x.uf_crm_1725574322 as status_do_pagamento,
	x.uf_crm_1725548469220 as data_solicitacao_upsell,
	x.uf_crm_1726002322164 as expectativa_com_o_projeto,
	x.uf_crm_1726003734 as status_do_projeto,
	x.uf_crm_1729086829554 as detalhes_motivo_cancelamento,
	x.uf_crm_1729086887055 as data_solicitacao_cancelamento,
	x.uf_crm_1732567021643 as como_avalia_implantacao_bendito,
	x.uf_crm_1733491265514 as novo_valor,
	x.uf_crm_1738931838 as cliente_ja_possui_base_bendito,
	x.uf_crm_1741275511723 as quantidade_de_pedidos,
	x.uf_crm_1741275566502 as valor_faturado,
	x.uf_crm_1741275596050 as produto,
	x.uf_crm_1741275610428 as valor_vendido,
	x.uf_crm_1741275634394 as qtd_vendida,
	--x.uf_crm_66d0a6a12e620, -- não é usado
	--x.uf_crm_66d0a6a146d20, -- não é usado
	--x.uf_crm_66d0a6a1529fd, -- não é usado
	--x.uf_crm_66d0a6a15d94f, -- não é usado
	--x.uf_crm_66d0a6a169edc, -- não é usado
	x.uf_crm_deal_1719236124580 as plano,
	x.uf_crm_deal_1725540405848 as tem_desenvolvimento_bendito,
	x.uf_crm_deal_1725540759541 as tipo_de_desenvolvimento,
	x.uf_crm_deal_1725569571488 as expectativa_valor_recorrencia,
	x.uf_crm_deal_1725569886689 as pessoa_responsavel_pelo_lead,
	x.uf_crm_deal_1727374223411 as qual_a_dor_do_cliente,
	x.uf_crm_deal_1727374263239 as ja_utilizavam_solucao,
	x.uf_crm_deal_1732567150103 as conta_pra_gente_porque,
	x.uf_crm_deal_1738069738944 as tipo_de_produto_que_a_empresa_vende,
	x.uf_crm_deal_1738069820827 as como_fazem_gestao_de_pedidos_hj,
	x.uf_crm_deal_1722632496832 as email_da_pessoa_ponto_focal--,
	--x.utm_medium, --não é usado
	--x.utm_source, --não é usado
	--x.utm_term, -- não é usado
	--x.utm_content, -- não é usado
	--x.utm_campaign -- não é usado
from
	"bendito_intelligence_dev"."bitrix"."btx_processed_deal" x
left join "bendito_intelligence_dev"."bitrix"."btx_processed_deal_stage" x1
	on x.stage_id = x1.status_id
left join "bendito_intelligence_dev"."bitrix"."btx_processed_user" x2
	on x.assigned_by_id = x2."id"
left join "bendito_intelligence_dev"."bitrix"."btx_processed_user" x3
	on x.moved_by_id = x3."id"
left join "bendito_intelligence_dev"."bitrix"."btx_processed_user" x4
	on x.modify_by_id = x4."id"
left join "bendito_intelligence_dev"."bitrix"."btx_processed_user" x5
	on x.created_by_id = x5."id"
left join "bendito_intelligence_dev"."bitrix"."btx_processed_user" x6
	on x.last_activity_by = x6."id"