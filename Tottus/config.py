DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone',
}

TOTTUS_STORES = {
    'query_stores': "select distinct store_id from landzone.tottus_stores",
    'query_stores_prod': "select value from ctrl.scrapers_activation where retail = 'Tottus' and data_type = 'Store' and activate= true",
    'table_lz': 'tottus_stores'       
}

TOTTUS_CATEGORY = {
    'query_category': "select distinct subcategory_slug from landzone.tottus_category",
    'query_category_prod': "select value from ctrl.scrapers_activation where retail = 'Tottus' and data_type = 'Category' and activate= true",
    'table_lz': 'tottus_category'       
}

TOTTUS_PRODUCT_DETAIL = {
    'query_adress': "",
    'url': 'https://cornershopapp.com/es-cl/',
    'table_lz': 'tottus_product_detail'       
}

TOTTUS_GEOLOCATION = {
    'query_adress': "",
    'table_lz': 'geolocation'       
}


