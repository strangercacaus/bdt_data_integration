SELECT
	id,
	title AS titulo,
	stage_id AS id_estagio,
	category_id as id_categoria, -- nenhum deal possui este campo preenchido
	opportunity AS mrr,
	company_id as id_empresa,
	contact_id as id_contato,
	assigned_by_id AS id_atribuido_por,
	opened AS aberto,
	begin_date AS data_de_inicio,
	closed AS encerrado,
	close_date AS data_de_encerramento,
	date_create AS criado_em,
	created_by_id AS id_criado_por,
	date_modify AS id_modificado_em,
	modify_by_id AS id_modificdo_por,
	moved_time AS movido_em,
	moved_by_id AS id_movido_por,
	last_activity_time AS ultima_atividade_em,
	last_activity_by AS id_ultima_atividade_por,
	is_new as recente, -- provavelmente indica se o deal é recente
	lead_id AS id_lead,
	type_id AS id_tipo,
	comments AS comentarios,
	quote_id as id_orcamento, -- provavelmente o id do orçamento
	origin_id as id_origem, -- nenhum valor preenchido
	source_id AS id_da_fonte,
	source_description as descricao_da_fonte, -- não utilizado
	tax_value as valor_imposto, -- valor do imposto
	currency_id AS moeda,
	location_id as id_localizacao, -- não utilizado
	probability as chance_fechamento, -- provavelmente probabilidade de fechamento, não utilizado
	is_recurring AS recorrente,
	additional_info as informacoes_adicionais, -- não utilizado
	stage_semantic_id as id_estagio_semantico, -- não utilizado
	originator_id as id_originador, -- não utilizado
	is_manual_opportunity as oportunidade_manual,
	is_return_customer as cliente_retorno,
	is_repeated_approach as abordagem_repetida,
	uf_crm_1744677619 AS id_customer,
	uf_crm_1718891707656 AS cp_quantidade_de_usuarios,
	uf_crm_1718893063318 AS cp_ponto_focal,
	uf_crm_1721248408791 AS cp_email_ponto_focal,
	uf_crm_1718895613676 AS cp_erp,
	uf_crm_1718895632531 AS cp_segmento,
	uf_crm_1718895837277 AS cp_perfil,
	uf_crm_1719234554494 AS cp_email_financeiro,
	uf_crm_1719234611470 AS cp_forma_pgto_implementacao,
	uf_crm_1719234653377 AS cp_forma_pgto_recorrencia,
	uf_crm_1719851689133 AS cp_vr_negociado_recorrencia,
	uf_crm_1719851724735 AS cp_vr_negociado_implementacao,
	uf_crm_1719942627227 AS cp_anexar_o_contrato,
	uf_crm_1720015348995 AS cp_contrato_do_setup,
	uf_crm_1720198477652 AS cp_valor_negociado_ativacao,
	uf_crm_1720198502628 AS cp_valor_negociado_integracao,
	uf_crm_1721083795801 AS cp_quantidade_usuarios_ativos,
	uf_crm_1721083820061 AS cp_vr_transacionado_em_janeiro,
	uf_crm_1721083836488 AS cp_vr_transacionado_em_fevereiro,
	uf_crm_1721083849804 AS cp_vr_transacionado_em_marco,
	uf_crm_1721083861185 AS cp_vr_transacionado_em_abril,
	uf_crm_1721083874978 AS cp_vr_transacionado_em_maio,
	uf_crm_1721083894608 AS cp_vr_transacionado_em_junho,
	uf_crm_1721083906720 AS cp_vr_transacionado_em_julho,
	uf_crm_1724264534445 AS cp_vr_transacionado_em_agosto,
	uf_crm_1726007497 AS cp_vr_transacionado_em_setembro,
	uf_crm_1728416073106 AS cp_vr_transacionado_em_outubro,
	uf_crm_1733168617374 AS cp_vr_transacionado_novembro,
	uf_crm_1721226126 AS cp_modulo_do_sistema,
	uf_crm_1721227124 AS cp_ambiente,
	uf_crm_1721241190 AS cp_classificacao_do_atendimento,
	uf_crm_1721243220370 AS cp_area,
	uf_crm_1721243302036 AS cp_descricao,
	uf_crm_1721243367 AS cp_arquivos,
	uf_crm_1721244413245 AS cp_carro_chefe,
	uf_crm_1721244451568 AS cp_faturamento_medio_mensal,
	uf_crm_1721244493157 AS cp_qtd_media_pedidos_mes,
	uf_crm_1721244526025 AS cp_vendedor_destaque,
	uf_crm_1721244560622 AS cp_pretende_estar_operando_em,
	uf_crm_1721244826264 AS cp_motivo_do_cancelamento,
	uf_crm_1721244999810 AS cp_o_que_poderia_ter_sido_diferente,
	uf_crm_1721248388070 AS cp_tipo_de_aumento_de_receita,
	uf_crm_1721406278811 AS cp_data_do_churn,
	uf_crm_1724261215 AS cp_media_de_faturamento,
	uf_crm_1725574177266 AS cp_id_asaas,
	uf_crm_1725574322 AS cp_status_do_pagamento,
	uf_crm_1725548469220 AS cp_data_solicitacao_upsell,
	uf_crm_1726002322164 AS cp_expectativa_com_o_projeto,
	uf_crm_1726003734 AS cp_status_do_projeto,
	uf_crm_1729086829554 AS cp_detalhes_motivo_cancelamento,
	uf_crm_1729086887055 AS cp_data_solicitacao_cancelamento,
	uf_crm_1732567021643 AS cp_como_avalia_implantacao_bendito,
	uf_crm_1733491265514 AS cp_novo_valor,
	uf_crm_1738931838 AS cp_cliente_ja_possui_base_bendito,
	uf_crm_1741275511723 AS cp_quantidade_de_pedidos,
	uf_crm_1741275566502 AS cp_valor_faturado,
	uf_crm_1741275596050 AS cp_produto,
	uf_crm_1741275610428 AS cp_valor_vendido,
	uf_crm_1741275634394 AS cp_qtd_vendida,
	--uf_crm_66d0a6a12e620, -- não é usado
	--uf_crm_66d0a6a146d20, -- não é usado
	--uf_crm_66d0a6a1529fd, -- não é usado
	--uf_crm_66d0a6a15d94f, -- não é usado
	--uf_crm_66d0a6a169edc, -- não é usado
	uf_crm_deal_1719236124580 AS cp_plano,
	uf_crm_deal_1725540405848 AS cp_tem_desenvolvimento_bendito,
	uf_crm_deal_1725540759541 AS cp_tipo_de_desenvolvimento,
	uf_crm_deal_1725569571488 AS cp_expectativa_valor_recorrencia,
	uf_crm_deal_1725569886689 AS cp_pessoa_responsavel_pelo_lead,
	uf_crm_deal_1727374223411 AS cp_qual_a_dor_do_cliente,
	uf_crm_deal_1727374263239 AS cp_ja_utilizavam_solucao,
	uf_crm_deal_1732567150103 AS cp_conta_pra_gente_porque,
	uf_crm_deal_1738069738944 AS cp_tipo_de_produto_que_a_empresa_vende,
	uf_crm_deal_1738069820827 AS cp_como_fazem_gestao_de_pedidos_hj,
	uf_crm_deal_1722632496832 AS cp_email_da_pessoa_ponto_focal,
	utm_medium, --não é usado
	utm_source, --não é usado
	utm_term, -- não é usado
	utm_content, -- não é usado
	utm_campaign -- não é usado
FROM
	{{ ref('btx_processed_crm_deal') }}