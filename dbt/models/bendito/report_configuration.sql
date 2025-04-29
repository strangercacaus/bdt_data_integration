SELECT ("CONTENT"->>'id')::integer AS id,
("CONTENT"->>'id_customer')::integer AS id_customer,
("CONTENT"->>'catalog_cover_background_image')::character varying AS catalog_cover_background_image,
("CONTENT"->>'catalog_cover_text')::character varying AS catalog_cover_text,
("CONTENT"->>'catalog_banner')::character varying AS catalog_banner,
("CONTENT"->>'catalog_email')::character varying AS catalog_email,
("CONTENT"->>'catalog_phone')::character varying AS catalog_phone,
("CONTENT"->>'catalog_whatsapp')::character varying AS catalog_whatsapp,
("CONTENT"->>'catalog_instagram')::character varying AS catalog_instagram,
("CONTENT"->>'catalog_facebook')::character varying AS catalog_facebook
FROM {{source('bendito','bdt_raw_report_configuration')}}