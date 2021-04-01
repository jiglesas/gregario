DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone',
    'schema_spot': 'spot',
}

JUMBO_CATEGORY = {
    'url': 'https://www.jumbo.cl/',
    'query_category_prod': "select value from ctrl.scrapers_activation where retail = 'Jumbo' and data_type = 'Category' and activate= true",
    'table_lz': 'jumbo_category'       
}

JUMBO_STORES = {
    'query_stores':"select distinct id from landzone.jumbo_stores where activated = true",
    'query_stores_prod': "select value->>'store_id' from ctrl.scrapers_activation where retail = 'Jumbo' and data_type = 'Store' and activate= true",
    'query_stores_pill' : "select A.value->>'store_name' from ctrl.scrapers_activation A left join (select distinct store_name from landzone.jumbo_product_list a) B on B.store_name = A.value->>'store_name' where A.retail = 'Jumbo' and A.data_type = 'Store' and B.store_name is null",
    'url': 'https://www.jumbo.cl/',
    'table_lz': 'jumbo_stores',
    'table_v2': 'jumbo_stores_v2'     
}

JUMBO_PRODUCT_LIST = {
    'query_category':"select distinct category_url_sanitize from landzone.process_jumbo_category where activate = true",    
    'table_lz': 'jumbo_product_list'
}

JUMBO_PRODUCT_DETAIL = {
    'query_product':"select product_url_sanitize from landzone.process_jumbo_product_list where activate = true",    
    'table_lz': 'jumbo_product_detail',
    'table_img': 'jumbo_product_detail_img'
}

JUMBO_PRODUCT_LIST_ALL = {
    'query_category':"select distinct category_url_sanitize from landzone.process_jumbo_category where activate = true",    
    'table_lz': 'jumbo_product_list_all',
    'table_lz_selected': 'jumbo_product_list_selected'
}

#consulta que obtiene las tiendas faltantes en landzone
query = "select A.value->>'store_name' from ctrl.scrapers_activation A left join (select distinct store_name from landzone.jumbo_product_list a) B on B.store_name = A.value->>'store_name' where A.retail = 'Jumbo' and A.data_type = 'Store' and B.store_name is null"
