
  
    

  create  table "bendito_intelligence_dev"."bitrix"."btx_curated_deal__dbt_tmp"
  
  
    as
  
  (
    
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
	created_by_id AS criado_por,
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
	quote_id, -- provavelmente o id do orçamento
	origin_id, -- nenhum valor preenchido
	source_id id_da_fonte,
	source_description as descricao_da_fonte, -- não utilizado
	tax_value as valor_imposto, -- valor do imposto
	currency_id AS moeda,
	location_id as id_localizacao, -- não utilizado
	probability as chance_fechamento, -- provavelmente probabilidade de fechamento, não utilizado
	is_recurring AS recorrente,
	additional_info as informacoes_adicionais, -- não utilizado
	--stage_semantic_id, -- não utilizado
	--originator_id, -- não utilizado
	is_manual_opportunity as oportunidade_manual,
	is_return_customer as cliente_retorno,
	is_repeated_approach as abordagem_repetida,
	uf_crm_1718891707656 AS quantidade_de_usuarios,
	uf_crm_1718893063318 AS ponto_focal,
	uf_crm_1721248408791 AS email_ponto_focal,
	uf_crm_1718895613676 AS erp,
	uf_crm_1718895632531 AS segmento,
	uf_crm_1718895837277 AS perfil,
	uf_crm_1719234554494 AS email_financeiro,
	uf_crm_1719234611470 AS forma_pgto_implementacao,
	uf_crm_1719234653377 AS forma_pgto_recorrencia,
	uf_crm_1719851689133 AS vr_negociado_recorrencia,
	uf_crm_1719851724735 AS vr_negociado_implementacao,
	uf_crm_1719942627227 AS anexar_o_contrato,
	uf_crm_1720015348995 AS contrato_do_setup,
	uf_crm_1720198477652 AS valor_negociado_ativacao,
	uf_crm_1720198502628 AS valor_negociado_integracao,
	uf_crm_1721083795801 AS quantidade_usuarios_ativos,
	uf_crm_1721083820061 AS vr_transacionado_em_janeiro,
	uf_crm_1721083836488 AS vr_transacionado_em_fevereiro,
	uf_crm_1721083849804 AS vr_transacionado_em_marco,
	uf_crm_1721083861185 AS vr_transacionado_em_abril,
	uf_crm_1721083874978 AS vr_transacionado_em_maio,
	uf_crm_1721083894608 AS vr_transacionado_em_junho,
	uf_crm_1721083906720 AS vr_transacionado_em_julho,
	uf_crm_1724264534445 AS vr_transacionado_em_agosto,
	uf_crm_1726007497 AS vr_transacionado_em_setembro,
	uf_crm_1728416073106 AS vr_transacionado_em_outubro,
	uf_crm_1733168617374 AS vr_transacionado_novembro,
	uf_crm_1721226126 AS modulo_do_sistema,
	uf_crm_1721227124 AS ambiente,
	uf_crm_1721241190 AS classificacao_do_atendimento,
	uf_crm_1721243220370 AS area,
	uf_crm_1721243302036 AS descricao,
	uf_crm_1721243367 AS arquivos,
	uf_crm_1721244413245 AS carro_chefe,
	uf_crm_1721244451568 AS faturamento_medio_mensal,
	uf_crm_1721244493157 AS qtd_media_pedidos_mes,
	uf_crm_1721244526025 AS vendedor_destaque,
	uf_crm_1721244560622 AS pretende_estar_operando_em,
	uf_crm_1721244826264 AS motivo_do_cancelamento,
	uf_crm_1721244999810 AS o_que_poderia_ter_sido_diferente,
	uf_crm_1721248388070 AS tipo_de_aumento_de_receita,
	uf_crm_1721406278811 AS data_do_churn,
	uf_crm_1724261215 AS media_de_faturamento,
	uf_crm_1725574177266 AS id_asaas,
	uf_crm_1725574322 AS status_do_pagamento,
	uf_crm_1725548469220 AS data_solicitacao_upsell,
	uf_crm_1726002322164 AS expectativa_com_o_projeto,
	uf_crm_1726003734 AS status_do_projeto,
	uf_crm_1729086829554 AS detalhes_motivo_cancelamento,
	uf_crm_1729086887055 AS data_solicitacao_cancelamento,
	uf_crm_1732567021643 AS como_avalia_implantacao_bendito,
	uf_crm_1733491265514 AS novo_valor,
	uf_crm_1738931838 AS cliente_ja_possui_base_bendito,
	uf_crm_1741275511723 AS quantidade_de_pedidos,
	uf_crm_1741275566502 AS valor_faturado,
	uf_crm_1741275596050 AS produto,
	uf_crm_1741275610428 AS valor_vendido,
	uf_crm_1741275634394 AS qtd_vendida,
	--uf_crm_66d0a6a12e620, -- não é usado
	--uf_crm_66d0a6a146d20, -- não é usado
	--uf_crm_66d0a6a1529fd, -- não é usado
	--uf_crm_66d0a6a15d94f, -- não é usado
	--uf_crm_66d0a6a169edc, -- não é usado
	uf_crm_deal_1719236124580 AS plano,
	uf_crm_deal_1725540405848 AS tem_desenvolvimento_bendito,
	uf_crm_deal_1725540759541 AS tipo_de_desenvolvimento,
	uf_crm_deal_1725569571488 AS expectativa_valor_recorrencia,
	uf_crm_deal_1725569886689 AS pessoa_responsavel_pelo_lead,
	uf_crm_deal_1727374223411 AS qual_a_dor_do_cliente,
	uf_crm_deal_1727374263239 AS ja_utilizavam_solucao,
	uf_crm_deal_1732567150103 AS conta_pra_gente_porque,
	uf_crm_deal_1738069738944 AS tipo_de_produto_que_a_empresa_vende,
	uf_crm_deal_1738069820827 AS como_fazem_gestao_de_pedidos_hj,
	uf_crm_deal_1722632496832 AS email_da_pessoa_ponto_focal --,
	--utm_medium, --não é usado
	--utm_source, --não é usado
	--utm_term, -- não é usado
	--utm_content, -- não é usado
	--utm_campaign -- não é usado
FROM
	"bendito_intelligence_dev"."bitrix"."btx_processed_deal"
  );
  