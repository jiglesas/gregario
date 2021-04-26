DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone',
}

LIDER_CATEGORY = {
    'url': 'https://www.lider.cl/supermercado/',
    'table_lz': 'lider_category'       
}

LIDER_STORES = {
    'url': 'https://www.lider.cl/supermercado/',
    'query_stores':"select * from landzone.lider_stores where store_id = '5'",
    'query_stores_prod': "select value from ctrl.tmp_scrapers_activation where retail = 'Lider' and data_type = 'Store' and activate= true",
    'table_lz': 'lider_stores'       
}

LIDER_PRODUCT_LIST = {
    #'query_category':"select distinct url from landzone.test where activated= true", 
    'query_category':"select distinct category_url_sanitize from landzone.process_lider_category where activate= true",
    'query_category_prod':"select value from ctrl.tmp_scrapers_activation where retail = 'Lider' and data_type = 'Category' and activate= true",
    'table_lz': 'lider_product_list'
}

LIDER_PRODUCT_NUMBER = {
    'query_store':"""
            select distinct 
            	b.store_id , b.store_name
            from 
            	history.lider_product_detail a
            join
            	history.lider_store b
            on
            	a.store_id::numeric = b.store_id 
                """,
    'table_lz': 'lider_product_status'
}

LIDER_PRODUCT_DETAIL = {
    'query_product':"select distinct product_url_sanitize from landzone.process_lider_product_list where activate= true",    
    'table_lz': 'lider_product_detail',
    'table_img': 'lider_product_detail_img'
}

LIDER_PRODUCT_LIST_ALL = {
    #'query_category':"select distinct url from landzone.test where activated= true", 
    'query_category':"select distinct category_url_sanitize from landzone.process_lider_category",    
    'table_lz': 'lider_product_list_all'
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