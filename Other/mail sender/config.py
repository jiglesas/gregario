TUNNEL_SSH = {
    'host': '167.71.178.175',
    'user': 'thomas',
    'password': 'th1234th',
    'rebind_address': '10.132.176.238',
    'rebind_address_port': 5432,
}

DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'spot',
}

EMAIL_CONFIG = {
    'fromaddr': 'jiglesias@gregario.cl',
    'frompwd': '',
    'toaddr': 'pes4dill4@gmail.com',
     ##  'toaddr':'listadeptotecnologiaysoportecallcenter@agrosuper.com, matias@gregario.cl, ignacio@gregario.cl, cristobal@gregario.cl',
}


SQL = {
   'query_trutro': '''
        select distinct 
        	B.organization_name as "Organización"
        	, B.store_name as "Tienda"
        	, B.adress as "Dirección"
        	, A.name as "Producto"
        	, A.package as "Formato"
        	, A.brand as "Marca"
        	, (case when A.status = 'OUT_OF_STOCK' then 'Sin Stock'
        			when A.status = 'FREQUENTLY_OUT_OF_STOCK' then 'Probabilidad de quiebre'
        			end) as "Estado"
        	, create_date::timestamp as "Fecha"
        from -- select * from
        	landzone.cornershop_product_detail A
        left join 
        	landzone.cornershop_global B 
        on 
        	A.store_id = B.store_id 
        where 
        	brand in (
        	'La Española'
        	, 'La Espaã‘Ola'
        	, 'Mister'
        	, 'Mister Veggie'
        	, 'Mr Veggie'
        	, 'Mr. Veggie'
        	, 'Pf'
        	, 'Pf'
        	, 'PF Listo'
        	, 'Receta del Abuelo'
        	, 'TIL'
        	)
        	and status IN ('OUT_OF_STOCK', 'FREQUENTLY_OUT_OF_STOCK')
        	and
        	A.store_id::varchar(200)||A.id::varchar(200)||(create_date::timestamp)::VARCHAR(200) in
        	(
        	select 
        		A.store_id::varchar(200)||A.id::varchar(200)||MAX(create_date::timestamp)::VARCHAR(200)
        	from 
        		landzone.cornershop_product_detail A
        	group by 
        		A.store_id,A.id 
        	)
        	
        
        union
        
        select distinct 
        	B.organization_name as "Organización"
        	, B.store_name as "Tienda"
        	, B.adress as "Dirección"
        	, A.name as "Producto"
        	, A.package as "Formato"
        	, A.brand as "Marca"
        	, (case when A.status = 'OUT_OF_STOCK' then 'Sin Stock'
        			when A.status = 'FREQUENTLY_OUT_OF_STOCK' then 'Probabilidad de quiebre'
        			end) as "Estado"
        	, create_date::timestamp as "Fecha"
        from -- select * from
        	landzone.cornershop_product_detail A
        left join -- select * from
        	landzone.cornershop_global B 
        on 
        	A.store_id = B.store_id 
        where 
        	(aisle_id like ('%C_20%')
        	or aisle_id like ('%C_531%'))
        	and brand in (
        	'Lider'
        	, 'Líder'
        	)
        	and status IN ('OUT_OF_STOCK', 'FREQUENTLY_OUT_OF_STOCK')
        	and
        	A.store_id::varchar(200)||A.id::varchar(200)||(create_date::timestamp)::VARCHAR(200) in
        	(
        	select 
        		A.store_id::varchar(200)||A.id::varchar(200)||MAX(create_date::timestamp)::VARCHAR(200)
        	from 
        		landzone.cornershop_product_detail A
        	group by 
        		A.store_id,A.id 
        	)
    '''
}