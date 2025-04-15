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
	x2.name as estagio,
	x.mrr,
	x.id_empresa,
	x.id_contato,
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
	x.id_lead,
	x.id_tipo,
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
	x.cp_forma_pgto_implementacao as forma_pgto_implementacao,
	x.cp_forma_pgto_recorrencia as forma_pgto_recorrencia,
	x.cp_vr_negociado_recorrencia as vr_negociado_recorrencia,
	x.cp_vr_negociado_implementacao as vr_negociado_implementacao,
	x.cp_anexar_o_contrato as anexar_o_contrato,
	x.cp_contrato_do_setup as contrato_do_setup,
	x.cp_valor_negociado_ativacao as valor_negociado_ativacao,
	x.cp_valor_negociado_integracao as valor_negociado_integracao,
	x.cp_quantidade_usuarios_ativos as quantidade_usuarios_ativos,,
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
	x.cp_data_solicitacao_cancelamento,
	x.cp_como_avalia_implantacao_bendito,
	x.cp_novo_valor,
	x.cp_cliente_ja_possui_base_bendito,
	x.cp_quantidade_de_pedidos,
	x.cp_valor_faturado,
	x.cp_produto,
	x.cp_valor_vendido,
	x.cp_qtd_vendida,
	x.cp_plano,
	x.cp_tem_desenvolvimento_bendito,
	x.cp_tipo_de_desenvolvimento,
	x.cp_expectativa_valor_recorrencia,
	x.cp_pessoa_responsavel_pelo_lead,
	x.cp_qual_a_dor_do_cliente,
	x.cp_ja_utilizavam_solucao,
	x.cp_conta_pra_gente_porque,
	x.cp_tipo_de_produto_que_a_empresa_vende,
	x.cp_como_fazem_gestao_de_pedidos_hj,
	x.cp_email_da_pessoa_ponto_focal
from
	bitrix.btx_curated_deal x
left join bitrix.btx_processed_dealcategory x1
	on x.id_categoria = x1.id
left join bitrix.btx_processed_dealcategory_stage x2
	on x.id_estagio = x2.status_id
left join bitrix.btx_curated_user x3
	on x.id_atribuido_por = x3.id
left join bitrix.btx_curated_user x4
	on x.id_modificdo_por  = x4.id
left join bitrix.btx_curated_user x5
	on x.id_ultima_atividade_por = x5.id
left join bitrix.btx_curated_user x6
	on x.id_criado_por = x6.id
left join bitrix.btx_curated_user x7
	on x.id_movido_por = x7.id
left join btx_curated_userfield x8
	on x.cp_motivo_do_cancelamento = unnest(x8.list)
where x.id_categoria = 0	