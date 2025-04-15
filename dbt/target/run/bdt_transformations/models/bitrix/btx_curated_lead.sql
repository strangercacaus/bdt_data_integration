
  
    

  create  table "bendito_intelligence"."bitrix"."btx_curated_lead__dbt_tmp"
  
  
    as
  
  (
    
select
	id,
	title as titulo,
	"name" as nome,
	last_name as sobrenome,
	second_name as segundo_nome,
	honorific as honorifico,
	post as cargo,
	birthdate as data_de_nascimento,
	(SELECT array_agg(e->>'VALUE') FROM jsonb_array_elements(email) AS e) as email,
	(SELECT array_agg(e->>'VALUE') FROM jsonb_array_elements(phone) AS e) as telefone,
	im as mensagem_instantanea,
	has_email as tem_email,
	has_phone as tem_telefone,
	has_imol as tem_imol,
	address as endereco,
	address_2 as endereco_2,
	address_city as cidade,
	address_country as pais,
	address_country_code as codigo_pais,
	address_loc_addr_id as id_endereco_local,
	address_postal_code as cep,
	address_province as provincia,
	address_region as regiao,
	company_id as id_empresa,
	company_title as nome_empresa,
	currency_id as id_moeda,
	status_id as id_status,
	status_description as descricao_status,
	status_semantic_id as id_semantico_status,
	opened as aberto,
	is_manual_opportunity as oportunidade_manual,
	is_return_customer as cliente_retorno,
	opportunity as oportunidade,
	date_create as criado_em,
	date_modify as modificado_em,
	date_closed as encerrado_em,
	last_activity_time as ultima_atividade_em,
	moved_time as movido_em,
	created_by_id as id_criado_por,
	modify_by_id as id_modificado_por,
	assigned_by_id as id_atribuido_por,
	last_activity_by as id_ultima_atividade_por,
	moved_by_id as id_movido_por,
	contact_id as id_contato,
	source_id as id_origem,
	source_description as descricao_origem,
	origin_id as id_fonte,
	originator_id as id_originador,
	utm_source,
	utm_medium,
	utm_campaign,
	utm_content,
	utm_term,
	uf_crm_vk_wz as vk_wz,
	uf_crm_avito_wz as avito_wz,
	uf_crm_instagram_wz as instagram_wz,
	uf_crm_telegramid_wz as id_telegram_wz,
	uf_crm_telegramusername_wz as usuario_telegram_wz,
	"comments" as comentarios,
	(SELECT array_agg(e->>'VALUE') FROM jsonb_array_elements(link) AS e) as link
from "bendito_intelligence"."bitrix"."btx_processed_lead"
  );
  