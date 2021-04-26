# -*- coding: utf-8 -*-
DATABASE_CONFIG = {
    'host': '34.69.175.136',
    'dbname': 'pimvault',
    'user': 'jiglesias',
    'password': 'jigle2020',
    'port': '5432',
    'schema': 'landzone'
}

SISA_CATEGORY = {
    'query_category': "select distinct  aisle_value from landzone.sisa_category",
    'table_lz': 'sisa_category'       
}

SISA_STORES = {
    'query_stores':"select distinct seller from landzone.sisa_stores limit 8",
    'query_stores_prod': "select value->>'store_id' from ctrl.scrapers_activation where retail = 'Jumbo' and data_type = 'Store' and activate= true",
    'query_stores_pill' : "select A.value->>'store_name' from ctrl.scrapers_activation A left join (select distinct store_name from landzone.jumbo_product_list a) B on B.store_name = A.value->>'store_name' where A.retail = 'Jumbo' and A.data_type = 'Store' and B.store_name is null",
    'table_lz': 'sisa_stores'   
}

SISA_PRODUCT_LIST = {
    'query_stores_prod': "select value->>'store_id' from ctrl.scrapers_activation where retail = 'Jumbo' and data_type = 'Store' and activate= true",
    'query_stores_pill' : "select A.value->>'store_name' from ctrl.scrapers_activation A left join (select distinct store_name from landzone.jumbo_product_list a) B on B.store_name = A.value->>'store_name' where A.retail = 'Jumbo' and A.data_type = 'Store' and B.store_name is null",
    'table_lz': 'sisa_product_list'   
}

