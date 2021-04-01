DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'spot'
}

PEDIDOS = {
    'query': """
                select distinct 
                	A.section_id 
                	, A."name" 
                	, B.product_id 
                	, B.partner_id 
                	, b.business_type
                from 
                	history.pedidosya_category A
                join
                	(
                	select
                		b.partner_id 
                		, c.business_type
                		, b.section_id
                		, b.product_id 
                	from
                		history.pedidosya_product_detail B
                	join
                		history.pedidosya_store C
                	on 
                		B.partner_id = C.partner_id::varchar(200)
                	where
                		C.business_type = 'RESTAURANT' 
                		
                	) B
                on
                	A.section_id  = B.section_id 
 
                """,
    'table_lz': 'pedidosya_popup'       
}

PY_PRODUCT = {
    'query': "select distinct store_id, url, address from scrapinghub.pedidosya_store_v2 limit 5",
    'query_hy-2': "select distinct partner_id as store_id, url from history.pedidosya_store",
    'query_hy': """
                select distinct 
                	partner_id::varchar(200) as store_id
                	, url
                from 
                	history.pedidosya_store           
                     
                union
                            
                select distinct 
                	store_id::varchar(200)
                	, url
                from 
                	scrapinghub.pedidosya_store_v2
                where 
                	store_id not in (
                					select 
                						partner_id::varchar(200) 
                					from 
                					history.pedidosya_store a
					                )
                    """,
    'table_lz': 'pedidosya_product_detail_v2',  
    'table_error': 'pedidosya_product_detail_error'     
}
