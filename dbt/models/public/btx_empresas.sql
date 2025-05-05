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
	x.cp_id_no_cs as id_customer,
	x.titulo,
	x.cp_cnpj as cnpj,
	x.website,
	x.aberto,
	x.id_lead,
	x.receita,
	x.comentarios,
	x.industria,
	x.endereco_2,
	x.empregados,
	x.tem_email,
	x.tem_telefone,
	x.moeda,
	x.id_origem,
	x.minha_empresa,
	x.tipo_empresa,
	x.criado_em,
	x1.email as criado_por,
	x.modificado_em,
	x2.email as modificado_por,
	x3.email as atribuido_por,
	x.ultima_atividade_em,
	x4.email as ultima_atividade_por,
	x.cp_202408_p1_estudo_de_viabilidade,
	x.cp_202408_p2_estudo_de_viabilidade,
	x.cp_202408_p3_estudo_de_viabilidade,
	x.cp_site,
	x.cp_nome_fantasia,
	x.cp_total_de_pedidos_outubro,
	x.cp_principal_produto_outubro,
	x.cp_valor_faturado_outubro,
	x.cp_valor_vendido_do_principal_produto,
	x.cp_qntde_vendida_do_principal_produto,
	x.cp_status_da_empresa,
	x.cp_ha_quanto_tempo_a_empresa_existe
from
	bitrix.btx_crm_company x
left join bitrix.btx_user x1
	on x.id_criado_por = x1.id
left join bitrix.btx_user x2
	on x.id_modificado_por = x2.id
left join bitrix.btx_user x3
	on x.id_atribuido_por = x3.id
left join bitrix.btx_user x4
	on x.id_ultima_atividade_por = x4.id