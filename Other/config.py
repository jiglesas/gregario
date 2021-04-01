DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'ctrl'
}

UPLOAD = {
    'query_category_prod': "select value from ctrl.scrapers_activation where retail = 'Jumbo' and data_type = 'Category' and activate= true",
    'table_lz': 'homologation_all'       
}

PY_PRODUCT = {
    'query': "select distinct store_id, url, address from scrapinghub.pedidosya_store_v2 ",
    'table_lz': 'pedidosya_product_detail_v2'       
}

