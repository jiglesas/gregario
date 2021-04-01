DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone',
    'schema2': 'spot',
}

CORNERSHOP_STORES = {
    'query_stores': "select distinct id_sanitize from landzone.process_cornershop_stores",
    'query_farmacy': "select * from landzone.cornershop_global where country = 'CL' and (organization_name  like '%%Farmacias Ahum%%' or organization_name like '%%Salco%%') limit 180",
    'query_stores_selected': "select store_id from landzone.cornershop_global where store_id = '1' or store_id = '8'",
    'query_stores_selected_2': "select store_id from landzone.cornershop_global where store_id = '13456'",
    'query_stores_prod': "select value from ctrl.tmp_scrapers_activation where retail = 'Cornershop' and data_type = 'Store' and activate= true",
    'url': 'https://cornershopapp.com/es-cl/',
    'table_lz': 'cornershop_stores'
    
}

CORNERSHOP_STORES_MX = {
    'query_stores': "select distinct store_id_sanitize from landzone.process_cornershop_stores_mx where activate = true"       
}

CORNERSHOP_ADRESS = {
    'query_adress': "select distinct id from landzone.cornershop_stores",
    'url': 'https://cornershopapp.com/es-cl/',
    'table_lz': 'cornershop_adress'       
}

CORNERSHOP_GLOBAL = {
    'query_adress': "select distinct id from landzone.cornershop_stores",
    'url': 'https://cornershopapp.com/es-cl/',
    'table_lz': 'cornershop_global'       
}

CORNERSHOP_AISLES = {
    'query_aisles':"select distinct aisle_id_sanitize from landzone.process_cornershop_aisles where activate = true",
    'query_aisles_prod': "select value from ctrl.tmp_scrapers_activation where retail = 'Cornershop' and data_type = 'Category' and activate= true",
    'query_farmacy': "select aisle_id from spot.cornershop_aisles_farmacy where department_name <> 'Others'",
    'query_id_store': "select value->>'store_id' from ctrl.scrapers_activation where retail = 'Cornershop' and data_type = 'Store' and activate = true",
    'table_lz': 'cornershop_aisles',
    'table_spot': 'cornershop_aisles_farmacy'
    
}

CORNERSHOP_AISLES_MX = {
    'query_aisles':"select distinct aisle_id_sanitize from landzone.process_cornershop_aisles_mx where activate = true"
}

CORNERSHOP_PRODUCT_DETAIL = {
    'query_product':"select product_url_sanitize from landzone.process_corner_product_list where activate = true",    
    'table_lz': 'cornershop_product_detail',
    'table_farmacy': 'cornershop_product_detail_farmacy',
}

CORNERSHOP_PRODUCT_DETAIL_TEST = {
    'query_product':"select product_url_sanitize from landzone.process_corner_product_list where activate = true",    
    'table_lz': 'cornershop_product_detail_test'
}

CORNERSHOP_PRODUCT_DETAIL_ALL = {
    'query_product':"select product_url_sanitize from landzone.process_corner_product_list",    
    'table_lz': 'cornershop_product_detail_all',
    'table_lz_selected': 'cornershop_product_detail_all_selected',
}

CORNERSHOP_PRODUCT_MX = {
    'query_product':"select product_url_sanitize from landzone.process_corner_product_list",    
    'table_lz': 'cornershop_product_mx'
}

CORNERSHOP_PRODUCT_SPOT = {
    'query_product':"select product_url_sanitize from landzone.process_corner_product_list",    
    'table_lz': 'cornershop_product_selected'
}


FIREBASE_CONFIG = {
    'apiKey': 'AIzaSyApdGsjqJAuqa8BnheXz2wBZNqvbsSXfAQ',
    'authDomain': 'pimvault-293221.firebaseapp.com',
    'databaseURL': 'https://pimvault-293221.firebaseio.com',
    'projectId': 'pimvault-293221',
    'storageBucket': 'pimvault-293221.appspot.com',
    'messagingSenderId': '301495087004',
    'appId': '1:301495087004:web:a935defa8251998f366259',
    'measurementId': 'G-TVMWF9HGHZ'
}