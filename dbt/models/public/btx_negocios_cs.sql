{{ config(
	materialized = 'table',
	unique_key = 'id',
	enabled = true,
	post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ],
)}}
select
	x.id,
	x.titulo,
	x2.name as etapa,
	x.mrr,
	x.id_customer,
	x.id_empresa,
	x.id_contato,
	x.id_lead,
	x.id_tipo,
	x3.email as atribuido_por,
	x.aberto,
	x.data_de_inicio,
	x.encerrado,
	x.data_de_encerramento,
	x.criado_em,
	x6.email as criado_por,
	x.id_modificado_em,
	x4.email as modificdo_por,
	x.movido_em,
	x7.email as movido_por,
	x.ultima_atividade_em,
	x5.email as ultima_atividade_por,
	x.recente,
	x.comentarios,
	x.id_da_fonte,
	x.valor_imposto,
	x.oportunidade_manual,
	x.cliente_retorno,
	x.cp_quantidade_de_usuarios as quantidade_de_usuarios,
	x.cp_ponto_focal as ponto_focal,
	x.cp_email_ponto_focal as email_ponto_focal,
	x.cp_erp as erp,
	x.cp_segmento as segmento,
	x.cp_perfil as perfil,
	x.cp_email_financeiro as email_financeiro,
	(
		select 
			value 
		from bitrix.btx_crm_userfield_options o 
		where o.id = x.cp_forma_pgto_implementacao 
		and o.field_name = 'UF_CRM_1719234611470'
	) as forma_pgto_implementacao,
		(
		select 
			value 
		from bitrix.btx_crm_userfield_options o 
		where o.id = x.cp_forma_pgto_recorrencia 
		and o.field_name = 'UF_CRM_1719234653377'
	) as forma_pgto_recorrencia,
	x.cp_vr_negociado_recorrencia as vr_negociado_recorrencia,
	x.cp_vr_negociado_implementacao as vr_negociado_implementacao,
	case 
		when x.cp_anexar_o_contrato->>'downloadUrl' is not null
		then 'https://benditocs.bitrix24.com.br'|| (x.cp_anexar_o_contrato->>'downloadUrl')::varchar
		else null
	end as contrato_recorrencia,
	case 
		when x.cp_contrato_do_setup->>'downloadUrl' is not null
		then 'https://benditocs.bitrix24.com.br'|| (x.cp_anexar_o_contrato->>'downloadUrl')::varchar
		else null
	end as contrato_setup,
	x.cp_contrato_do_setup as contrato_do_setup,
	x.cp_valor_negociado_ativacao as valor_negociado_ativacao,
	x.cp_valor_negociado_integracao as valor_negociado_integracao,
	x.cp_quantidade_usuarios_ativos as quantidade_usuarios_ativos,
	x.cp_modulo_do_sistema modulo_do_sistema,
	x.cp_ambiente ambiente,
	x.cp_area,
	x.cp_descricao,
	x.cp_arquivos,
	x.cp_carro_chefe,
	x.cp_faturamento_medio_mensal,
	x.cp_qtd_media_pedidos_mes,
	x.cp_vendedor_destaque,
	x.cp_pretende_estar_operando_em,
	x.cp_motivo_do_cancelamento,
	x.cp_o_que_poderia_ter_sido_diferente,
	x.cp_tipo_de_aumento_de_receita,
	x.cp_data_do_churn,
	x.cp_media_de_faturamento,
	x.cp_id_asaas,
	x.cp_status_do_pagamento,
	x.cp_data_solicitacao_upsell,
	x.cp_expectativa_com_o_projeto,
	x.cp_status_do_projeto,
	x.cp_detalhes_motivo_cancelamento,
	x.cp_data_solicitacao_cancelamento as data_pedido_churn,
	--x.cp_como_avalia_implantacao_bendito,
	--x.cp_novo_valor,
	--x.cp_cliente_ja_possui_base_bendito,
	--x.cp_quantidade_de_pedidos,
	--x.cp_valor_faturado,
	--x.cp_produto,
	--x.cp_valor_vendido,
	--x.cp_qtd_vendida,
	array_to_string(
	ARRAY(
	SELECT o.value
	FROM jsonb_array_elements(x.cp_plano) e
	JOIN bitrix.btx_crm_userfield_options o 
	ON e::int = o.id 
	AND o.field_name = 'UF_CRM_DEAL_1719236124580'),';') AS plano,
	x.cp_tem_desenvolvimento_bendito::int::bool as tem_desenvolvimento_bendito,
	array_to_string(
	ARRAY(
	SELECT o.value
	FROM jsonb_array_elements(x.cp_tipo_de_desenvolvimento) e
	JOIN bitrix.btx_crm_userfield_options o 
	ON e::int = o.id 
	AND o.field_name = 'UF_CRM_DEAL_1725540759541'),'; ') AS tipo_de_desenvolvimento--,
	--x.cp_expectativa_valor_recorrencia as ,
	--x.cp_qual_a_dor_do_cliente,
	--x.cp_ja_utilizavam_solucao,
	--x.cp_conta_pra_gente_porque,
	--x.cp_tipo_de_produto_que_a_empresa_vende,
	--x.cp_como_fazem_gestao_de_pedidos_hj,
	--x.cp_email_da_pessoa_ponto_focal
from
	bitrix.btx_crm_deal x
left join bitrix.btx_processed_crm_dealcategory x1
	on x.id_categoria = x1.id
left join bitrix.btx_processed_crm_dealcategory_stage x2
	on x.id_estagio = x2.status_id
left join bitrix.btx_user x3
	on x.id_atribuido_por = x3.id
left join bitrix.btx_user x4
	on x.id_modificdo_por  = x4.id
left join bitrix.btx_user x5
	on x.id_ultima_atividade_por = x5.id
left join bitrix.btx_user x6
	on x.id_criado_por = x6.id
left join bitrix.btx_user x7
	on x.id_movido_por = x7.id