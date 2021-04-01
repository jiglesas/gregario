DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'spot',
    'schema2': 'history',
}

SQL = {
   'query': '''
       select distinct 
                B.organization_name as "Organización"
                , B.store_name as "Tienda"
                , B.address as "Dirección"
                , A.name as "Producto"
                , A.package as "Formato"
                , A.brand as "Marca"
                , (case when A.status = 'OUT_OF_STOCK' then 'Sin Stock'
                                when A.status = 'FREQUENTLY_OUT_OF_STOCK' then 'Probabilidad de quiebre'
                                end) as "Estado"
                , A.load_date::timestamp as "Fecha"
        from -- select * from
                history.cornershop_product_detail A
        left join 
                history.cornershop_store B 
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
                and A.load_date::date = current_date
                and
                A.store_id::varchar(200)||A.id::varchar(200)||(A.load_date::timestamp)::VARCHAR(200) in
                (
                select 
                        A.store_id::varchar(200)||A.id::varchar(200)||MAX(A.load_date::timestamp)::VARCHAR(200)
                from 
                        history.cornershop_product_detail A
                group by 
                        A.store_id,A.id 
                )
                
        
        union
        
        select distinct 
                B.organization_name as "Organización"
                , B.store_name as "Tienda"
                , B.address as "Dirección"
                , A.name as "Producto"
                , A.package as "Formato"
                , A.brand as "Marca"
                , (case when A.status = 'OUT_OF_STOCK' then 'Sin Stock'
                                when A.status = 'FREQUENTLY_OUT_OF_STOCK' then 'Probabilidad de quiebre'
                                end) as "Estado"
                , A.load_date::timestamp as "Fecha"
        from -- select * from
                history.cornershop_product_detail A
        left join -- select * from
                history.cornershop_store B 
        on 
                A.store_id = B.store_id 
        where 
                (aisle_id like ('%%C_20%%')
                or aisle_id like ('%%C_531%%'))
                and brand in (
                                'Lider'
                , 'Líder'
                )
                and status IN ('OUT_OF_STOCK', 'FREQUENTLY_OUT_OF_STOCK')
                and A.load_date::date = current_date
                and
                A.store_id::varchar(200)||A.id::varchar(200)||(A.load_date::timestamp)::VARCHAR(200) in
                (
                select 
                        A.store_id::varchar(200)||A.id::varchar(200)||MAX(A.load_date::timestamp)::VARCHAR(200)
                from 
                        history.cornershop_product_detail A
                group by 
                        A.store_id,A.id 
                )
    '''
}

SQL_CHAMPION = {
   'query': '''
        select 
        	*
        from 
        	(
        	select distinct
        		'Jumbo Maipu' as "Retail"
        		, A.product_name::varchar(300) as "Producto"
        		, A.formato::varchar(300) as "Formato"
        		, A.brand::varchar(300) as "Marca"
        		, A.status::varchar(300) as "Estado"
        		, A.load_date::varchar(300) as "Revision"
        	from -- select distinct brand from
        		history.jumbo_product_detail A
        	where
        		A.store_id = '25' 
        		and A.status <> 'Disponible' 
        		and A.brand in ('Champion', 'Champion Dog', 'Champion Cat', 'Sabrokan')
        		and A.load_date::date = current_date
        		and A.store_id::varchar(200)||A.product_id::varchar(200)||A.load_date::VARCHAR(200) in  
        			(
        			select 
        				A.store_id::varchar(200)||A.product_id::varchar(200)||MAX(A.load_date)::VARCHAR(200) 
        			from 
        				history.jumbo_product_detail A
        			where 
        				A.load_date::date = current_date
        				and A.brand in ('Champion', 'Champion Dog', 'Champion Cat', 'Sabrokan')
        				and A.product_name not in (
        					'Alimento perro adulto pollo y vegetales 9 kg' 
        					, 'Alimento perro recetas del chef de carne 283 g' 
        					, 'Alimento perro recetas del chef de pollo 283 g')

        			group by 
        	       		A.store_id, A.product_id 
        	       	)
        						
        	union	
        	
        	select distinct
        		'Cornershop Jumbo Maipu' as "Retail"
        		, A.name::varchar(300) as "Producto"
        		, A.package::varchar(300) as "Formato"
        		, A.brand::varchar(300) as "Marca"
        		, (case when A.status = 'OUT_OF_STOCK' then 'Agotado'
        	     		when A.status = 'FREQUENTLY_OUT_OF_STOCK' then 'Probabilidad de quiebre'
        	         end) as "Estado"
        		, A.load_date::varchar(300) as "Revision"
        	from 
        		history.cornershop_product_detail A
        	where 
        		A.store_id = '420' 
        		and A.status <> 'AVAILABLE' 
        		and A.brand in ('Champion Katt', 'Champion Dog', 'Champion cat', 'Champion Mini Pets', 'Sabrokan') -- AGREGAR MARCAS
        		and A.load_date::date = current_date
        		and store_id::varchar(200)||id::varchar(200)||load_date::varchar(200) in
        			(
        			select 
        				store_id::varchar(200)||id::varchar(200)||MAX(load_date)::VARCHAR(200) 
        			from 
        				history.cornershop_product_detail A
        			where 
        				A.load_date::date = current_date
        				and A.brand in ('Champion Katt', 'Champion Dog', 'Champion cat', 'Champion Mini Pets', 'Sabrokan')
        			group by 
        	       		A.store_id,A.id 
        	       	)
        	) A
        order by 
        	A."Estado" asc
       		
    '''
}

