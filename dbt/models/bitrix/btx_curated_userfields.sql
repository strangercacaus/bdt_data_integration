{{config(materialized='view')}}
select 'contact'as entidade, edit_form_label->>'br' as nome_de_exibicao, * from {{ref('btx_processed_contact_userfield')}}
union all
select 'company' as entidade, edit_form_label->>'br' as nome_de_exibicao,  * from {{ref('btx_processed_company_userfield')}}
union all
select 'deal' as entidade, edit_form_label->>'br' as nome_de_exibicao,  * from {{ref('btx_processed_deal_userfield')}}

