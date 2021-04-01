DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone',
    'schema_mx': 'spot',
}

RAPPI_STORES_ZONE = {
    'query_stores': "select distinct id_sanitize from landzone.process_cornershop_stores",
    'table_lz': 'rappi_stores_zone'       
}

RAPPI_STORES_DETAIL = {
    'query_stores': "select distinct store_id from landzone.rappi_stores_zone",
    'table_lz': 'rappi_stores_detail'       
}

RAPPI_PRODUCT_DETAIL = {
    'query_adress': "select distinct id from landzone.s",
    'table_lz': 'rappi_product_detail'       
}

RAPPI_AISLES = {
    'query_aisles':"select distinct aisle_id_sanitize from landzone.process_cornershop_aisles where activate = true",    
    'table_lz': 'cornershop_aisles'
}

RAPPI_STORES_MARKET = {
    'query_stores':"select distinct store_id from landzone.rappi_stores_market",
    'query_all': "select * from landzone.rappi_stores_market",
    'query_stores_prod': "select value from ctrl.scrapers_activation where retail = 'Rappi-Market' and data_type = 'Store' and activate= true", 
    'query_category_prod': "select value from ctrl.scrapers_activation where retail = 'Rappi-Market' and data_type = 'Category' and activate= true",  
    'table_lz': 'rappi_market_stores'
}

RAPPI_MARKET_AISLE = {
    'query_stores':"select distinct store_id from history.rappi_market_store",
    'query_stores_mx':"select distinct store_id from spot.rappi_stores_market_mx",
    'query_all': "select * from landzone.rappi_stores_market",    
    'table_lz': 'rappi_market_aisle',
    'table_spot': 'rappi_market_aisle_mx'
}

RAPPI_STORES_CONVENIENCE = {
    'query_stores':"select distinct store_id from history.rappi_convenience_store",
    'query_all': "select * from landzone.rappi_stores_market",    
    'table_lz': 'rappi_convenience_stores'
}

RAPPI_CONVENIENCE_AISLE = {
    'query_stores':"select distinct store_id from landzone.rappi_stores_convivency",
    'query_all': "select * from landzone.rappi_stores_market",    
    'table_lz': 'rappi_convenience_aisle'
}

RAPPI_CONVENIENCE_PRODUCT = {
    'query_stores':"select store_id from landzone.rappi_convenience_store",  
    'query_stores_prod_deprecated':"select value from ctrl.scrapers_activation where retail = 'Rappi-Convenience' and data_type = 'Category' and activate= true", 
    'query_stores_prod': """select value ->> 'store_id' as store_id, value ->> 'category_id' as category_id 
                            from ctrl.scrapers_activation
                            where retail = 'Rappi-Convenience' and data_type = 'Category' and activate = true""",
    'query_stores_prod_temp': """select value ->> 'store_id' as store_id, value ->> 'category_id' as category_id 
                            from ctrl.scrapers_activation
                            where retail = 'Rappi-Convenience' and data_type = 'Category'
                            and value ->> 'store_id' in (select store_id
                            								from history.rappi_convenience_store a
                            							where brand_id in (13804, 13811, 15019, 13699, 13641, 13645, 13671, 13668, 16907)
                            							and name <> 'error')""",
    'table_lz': 'rappi_convenience_product'
}

RAPPI_CONVENIENCE_SEARCH = {
    'query_stores':"select distinct * from landzone.rappi_convenience_stores",  
    'table_lz': 'rappi_convenience_product_experimental'
}

RAPPI_CONVENIENCE_SEARCH_ALL = {
    'query_stores':"select distinct store_id from landzone.rappi_convenience_stores", 
    'query_markets':"select distinct store_id from landzone.rappi_market_stores", 
    'table_lz': 'rappi_convenience_product_experimental_all'
}

GEO_MX = {
    'query_geo':"select * from spot.geolocation_mx", 
    'table_lz': 'geolocation_mx'
}

GEO = {
    #'query_geo':"select * from scrapinghub.geolocation_new_process where region = 'Metropolitana'", 
    'query_geo':"select * from scrapinghub.geolocation_new_process where commune in ('Concepción', 'Viña del Mar', 'temuco', 'Antofagasta', 'La Serena', 'Iquique', 'Rancagua', 'Coquimbo', 'Valparaiso', 'Hualpén', 'Talcahuano', 'Penco', 'San Pedro de la Paz', 'Concón', 'Quilpué', 'Villa Alemana', 'Padre Las Casas', 'Machalí', 'Yungay')",
    'table_lz': 'geolocation_mx'
}

RAPPI_STORES_MARKET_MX = {
    'query_stores':"select distinct store_id from landzone.rappi_stores_market",
    'query_all': "select * from landzone.rappi_stores_market",    
    'table_lz': 'rappi_stores_market_mx'
}

RAPPI_MARKET_PRODUCT = {
    'query_market':"select * from landzone.rappi_market_aisle where store_id = '900015692'",  
    'query_aisle_mx':"select distinct * from spot.rappi_market_aisle_mx",
    'table_lz': 'rappi_market_product_detail',
    'table_spot_mx': 'rappi_market_product_mx'
}

RAPPI_RESTAURANT_PRODUCT = {
    'query_stores':"select distinct store_id, lat, lng from landzone.rappi_stores_detail",  
    'table_lz': 'rappi_restaurant_product'
}

