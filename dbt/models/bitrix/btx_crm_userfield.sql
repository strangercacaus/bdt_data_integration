{{config(materialized='view')}}
select 'contact'as entidade, edit_form_label->>'br' as nome_de_exibicao, * from {{ref('btx_processed_crm_contact_userfield')}}
union all
select 'company' as entidade, edit_form_label->>'br' as nome_de_exibicao,  * from {{ref('btx_processed_crm_company_userfield')}}
union all
select 'deal' as entidade, edit_form_label->>'br' as nome_de_exibicao,  * from {{ref('btx_processed_crm_deal_userfield')}}

