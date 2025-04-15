
  
    

  create  table "bendito_intelligence"."bitrix"."btx_curated_product__dbt_tmp"
  
  
    as
  
  (
    
select
	-- Identificadores primários
	id,
	"name" as nome,
	code as codigo,
	catalog_id as id_catalogo,
	section_id as id_secao,
	xml_id as id_xml,
	
	-- Informações do produto
	description as descricao,
	description_type as tipo_descricao,
	detail_picture as imagem_detalhe,
	preview_picture as imagem_preview,
	
	-- Preços e medidas
	price as preco,
	measure as medida,
	currency_id as id_moeda,
	vat_id as id_vat,
	vat_included,
	
	-- Status e ordenação
	active as ativo,
	sort as ordem,
	
	-- Datas e timestamps
	date_create as criado_em,
	timestamp_x,
	
	-- Referências de usuário
	created_by as id_criado_por,
	modified_by as id_modificado_por,
	
	-- Campos personalizados
	property_45 as property_45
from
	"bendito_intelligence"."bitrix"."btx_processed_product"
  );
  