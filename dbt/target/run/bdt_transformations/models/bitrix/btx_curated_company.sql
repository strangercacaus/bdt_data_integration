
  
    

  create  table "bendito_intelligence"."bitrix"."btx_curated_company__dbt_tmp"
  
  
    as
  
  (
    
select
	id,
	title as titulo,
	web ->0->>'VALUE' as website,
	opened as aberto,
	lead_id as id_lead,
	revenue as receita,
	"comments" as comentarios,
	industry as industria,
	address_2 as endereco_2,
	employees as empregados,
	has_email as tem_email,
	has_phone as tem_telefone,
	currency_id as moeda,
	origin_id as id_origem,
	is_my_company as minha_empresa,
	company_type as tipo_empresa,
	date_create as criado_em,
	created_by_id as id_criado_por,
	date_modify as modificado_em,
	modify_by_id as id_modificado_por,
	assigned_by_id as id_atribuido_por,
	last_activity_time as ultima_atividade_em,
	last_activity_by as id_ultima_atividade_por,
	uf_crm_1724941360 as cp_202408_p1_estudo_de_viabilidade,
	uf_crm_1724941486 as cp_202408_p2_estudo_de_viabilidade,
	uf_crm_1724941613 as cp_202408_p3_estudo_de_viabilidade,
	uf_crm_1720548388015 as cp_site,
	uf_crm_1726169579580 as cp_id_no_cs,
	uf_crm_1726169541171 as cp_nome_fantasia,
	uf_crm_1726169611418 as cp_total_de_pedidos_outubro,
	uf_crm_1726169646823 as cp_principal_produto_outubro,
	uf_crm_1726169666589 as cp_valor_faturado_outubro,
	uf_crm_1726173756398 as cp_valor_vendido_do_principal_produto,
	uf_crm_1726173773647 as cp_qntde_vendida_do_principal_produto,
	uf_crm_1726940183689 as cp_status_da_empresa,
	uf_crm_company_1720014986901 as cp_cnpj,
	uf_crm_company_1738069980122 as cp_ha_quanto_tempo_a_empresa_existe
from
	"bendito_intelligence"."bitrix"."btx_processed_company"
  );
  