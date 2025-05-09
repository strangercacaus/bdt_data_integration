select
	id,
	date_create as criado_em,
	created_by_id as id_criado_por,
	date_modify as modificado_em,
	modify_by_id as id_modificado_por,
	assigned_by_id as id_atribuido_por,
	last_activity_time as ultima_atividade_em,
	last_activity_by as id_ultima_atividade_por,
	"name" as nome,
	last_name as sobrenome,
	second_name as segundo_nome,
	post as cargo,
	honorific as honorifico,
	has_email as tem_email,
	(SELECT array_agg(e->>'VALUE') FROM jsonb_array_elements(email) AS e) AS email,
	has_phone as tem_telefone,
	(SELECT array_agg(e->>'VALUE') FROM jsonb_array_elements(phone) AS e) as telefone,
	lead_id as id_lead,
	company_id as id_company,
	type_id as id_tipo,
	origin_id as id_origem,
	source_id as id_fonte,
	originator_id as id_originador,
	face_id as face_id,
	photo as foto,
	export as exportado,
	opened as aberto,
	address as endereco,
	address_2 as endereco_2,
	address_city as endereco_cidade,
	address_region as endereco_regiao,
	address_province as endereco_provincia,
	address_country as endereco_pais,
	address_postal_code as endereco_codigo_postal,
	address_loc_addr_id as endereco_id_da_localizacao,
	birthdate as data_de_nascimento,
	"comments" as comentarios,
	has_imol as tem_imol,
	origin_version as versao_origem,
	source_description as fonte_descricao,
	uf_crm_contact_1725540305021 as telefone_da_pessoa_ponto_focal,
	uf_crm_contact_1724942124895 as "cp_202408_p1_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942182263 as "cp_202408_p2_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942265895 as "cp_202408_p3_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942380589 as "cp_202408_p4_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942491293 as "cp_202408_p5_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942591655 as "cp_202408_p6_estudo_de_viabilidade_whatsapp",
	uf_crm_contact_1724942632826 as "cp_202408_p7_estudo_de_viabilidade_whatsapp",
	utm_term,
	utm_content,
	utm_medium,
	utm_source,
	utm_campaign,
	uf_crm_vk_wz,
	uf_crm_avito_wz,
	uf_crm_instagram_wz,
	uf_crm_telegramid_wz,
	uf_crm_telegramusername_wz
from
	{{ref('btx_processed_crm_contact')}}