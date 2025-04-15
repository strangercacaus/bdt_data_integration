
select
	id,
	"name" as nome,
	last_name as ultimo_nome,
	email,
	active as ativo,
	xml_id as id_xml,
	is_online as online,
	time_zone as fuso_horario,
	user_type as tipo_de_usuario,
	last_login as ultimo_login,
	work_phone as telefone_trabalho,
	timestamp_x as timestamp_x,
	personal_www as site_pessoal,
	date_register as data_de_registro,
	personal_city as cidade_pessoal,
	uf_department as departamento,
	work_position as cargo,
	personal_photo as foto_pessoal,
	uf_phone_number as numero_de_telefone,
	personal_gender as genero_pessoal,
	personal_mobile as numero_de_celular_pessoal,
	time_zone_offset as deslocamento_do_fuso_horario,
	personal_birthday as data_de_nascimento_pessoal,
	last_activity_date as data_da_ultima_atividade,
	uf_employment_date as data_de_contratacao
from
	"bendito_intelligence"."bitrix"."btx_processed_user"