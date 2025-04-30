select
	-- Identificadores primários
	id,
	subject as assunto,
	description as descricao,
	type_id as id_tipo,
	
	-- Datas e prazos
	created as criado_em,
	last_updated as atualizado_em,
	deadline as prazo,
	end_time as data_fim,
	start_time as data_inicio,
	
	-- Status e conclusão
	completed as concluido,
	status as status,
	status_code as codigo_status,
	priority as prioridade,
	autocomplete_rule as regra_autocompletar,
	
	-- Propriedade e responsabilidade
	owner_id as id_proprietario,
	owner_type_id as id_tipo_proprietario,
	responsible_id as id_responsavel,
	editor_id as id_editor,
	author_id as id_autor,
	
	-- Resultados
	result_source_id as id_fonte_resultado,
	result_currency_id as id_moeda_resultado,
	result_sum as soma_resultado,
	result_status as status_resultado,
	result_mark as marca_resultado,
	result_stream as fluxo_resultado,
	result_value as valor_resultado,
	
	-- Informações do provedor
	provider_id as id_provedor,
	provider_type_id as id_tipo_provedor,
	provider_group_id as id_grupo_provedor,
	provider_params as parametros_provedor,
	provider_data as dados_provedor,
	
	-- Informações adicionais
	"location" as localizacao,
	direction as direcao,
	description_type as tipo_descricao,
	files as arquivos,
	error as erro,
	associated_entity_id as id_entidade_associada,
	url as url,
	settings as configuracoes,
	
	-- Notificações
	notify_value as valor_notificacao,
	notify_type as tipo_notificacao,
	
	-- Origem
	origin_id as id_origem,
	originator_id as id_originador
from {{ref('btx_processed_crm_activity')}}