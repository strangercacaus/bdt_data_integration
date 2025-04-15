{{ config(
    materialized = 'view',
	enabled = true,
    post_hook=[
        "GRANT SELECT ON {{ this }} TO bendito_metabase"
    ]
)}}
select
	x.id,
	x1.description as tipo,
	x2.description as status,
	x."token",
	x."language" as idioma,
	x3.first_name || ' ' || x3.last_name  as criado_por,
	x3.username as email_criado_por,
	x.time_creation as criado_em,
	x4.first_name || ' ' || x4.last_name  as modificado_por,
	x4.username as email_modificado_por,
	x.time_modification as modificado_em,
	x.time_free as periodo_gratis,
	x.asaas_customerid as id_asaas,
	x5.company_name as razao_social,
	x5.social_name as nome_fantasia,
	x5.registration_code as cnpj,
	x9.description as tipo_cobranca,
	x10.description as tipo_pagamento,
	x11.description as tipo_recorrencia,
	x.additional_data as dados_adicionais,
	x.invoice_notes as notas_boleto,
	x.time_status as status_alterado_em,
	x8.first_name || ' ' || x8.last_name as cs,
	x8.username as email_cs,
	x.churn_request_time as solicitou_churn_em,
	x.churn_confirm_time as confirmou_churn_em,
	array_to_string(
        ARRAY(
            SELECT x1.description
            FROM UNNEST(x.churn_reason) WITH ORDINALITY AS u(id, i)
            JOIN bendito.bdt_curated_domain x1 ON x1.value = u.id and x1.domain = 'MOTIVO_CHURN'
            ORDER BY i
            ),';'
    ) AS motivos_de_churn,
	x6.description as tipo_integracao,
	x.b2b_welcome_message as portal_msg_boas_vindas,
	x7.description as portal_assunto_email_boas_vindas,
	x7.mail as portal_email_boas_vindas,
	x.b2b_show_only_active_collections as portal_apenas_colecoes_ativas,
	x.b2b_faq_client as portal_faq,
	x.url_b2b as portal_url,
	x.b2b_messages_cart as portal_mensagem_carrinho,
	x.b2b_messages as portal_mensagem,
	x.catalog_cover_backgroundimage as portal_imagem_fundo_catalogo,
	x.catalog_cover_text as portal_texto_capa
from
	bendito.bdt_curated_customer x
left join bendito.bdt_curated_domain x1 on x.type = x1.value and x1.domain = 'TIPO_CLIENTE'
left join bendito.bdt_curated_domain x2 on x.type = x2.value and x2.domain = 'STATUS_CLIENTE'
left join bendito.bdt_curated_user x3 on x.id_user_creation = x3.id
left join bendito.bdt_curated_user x4 on x.id_user_modification = x4.id
left join bendito.bdt_curated_person x5 on x.person = x5.id
left join bendito.bdt_curated_domain x6 on x.integration_type = x6.value and x6.domain = 'TIPO_INTEGRACAO'
left join bendito.bdt_curated_mail_model x7 on x.b2b_invite_mail = x7.id
left join bendito.bdt_curated_user x8 on x.manager = x8.id
left join bendito.bdt_curated_domain x9 on x.integration_type = x9.value and x9.domain = 'TIPO_PAGAMENTO'
left join bendito.bdt_curated_domain x10 on x.integration_type = x10.value and x10.domain = 'FORMA_PAGAMENTO'
left join bendito.bdt_curated_domain x11 on x.integration_type = x11.value and x11.domain = 'TIPO_RECORRENCIA'
left join bendito.bdt_curated_domain x12 on x.integration_type = x12.value and x12.domain = 'ESTAGIO_CLIENTE'