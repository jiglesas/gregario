# -*- coding: utf-8 -*-
import pandas as pd
import config
import threading
import json
import io
import geo
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

result = []

#experimental
#https://services.rappi.cl/api/es-proxy/search/v2/products?page=1

def get_product(store_id, store_type):
    #{"query":"tika","stores":[581],"store_type":"ok_market_big","page":1,"size":40,"options":{},"helpers":{"home_type":"by_products","store_type_group":"Express","type":"by_categories"}}
    parameters = {"query":"tika","stores":[int(store_id)],"store_type": str(store_type),"page":1,"size":400,"options":{},"helpers":{"home_type":"by_products","store_type_group":"Express","type":"by_categories"}}
    
    url = 'https://services.rappi.cl/api/es-proxy/search/v2/products?page=1'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        print('ok')
        data = json.loads(y.text)
        
        '''
        components_len = len(data['data']['components'])
        for i in range(0, components_len):
            products_len = len(data['data']['components'][i]['resource']['products'])
            for j in range(0, products_len):
                product_id = data['data']['components'][i]['resource']['products'][j]['product_id']
                ean = data['data']['components'][i]['resource']['products'][j]['ean']
                name = data['data']['components'][i]['resource']['products'][j]['name']
                name = name.replace('\n', ' ')
                name = name.replace('\t', ' ')
                price = data['data']['components'][i]['resource']['products'][j]['price']
                product_type = data['data']['components'][i]['resource']['products'][j]['product_type']
                presentation = data['data']['components'][i]['resource']['products'][j]['presentation']
                retail_id = data['data']['components'][i]['resource']['products'][j]['retail_id']
                trademark = 'algo'
                trademark = trademark.replace('\n', ' ')
                trademark = trademark.replace('\t', ' ')
                status = data['data']['components'][i]['resource']['products'][j]['is_available']
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                data_in = {'creation_date': date, 'status': status}
                result.append(data_in)
            '''
        algo = len(data['hits'])
        #print(algo)
        for i in range(0, algo):
            product_id = data['hits'][i]['product_id']
            try:
                ean = data['hits'][i]['ean']
            except:
                ean = 'error'
                
            name = data['hits'][i]['name']
            name = name.replace('\n', ' ')
            name = name.replace('\r', ' ')
            name = name.replace('\t', ' ')
            description = data['hits'][i]['description']
            description = description.replace('\n', ' ')
            description = description.replace('\t', ' ')
            description = description.replace('\r', ' ')
            
            price = data['hits'][i]['price']
            try:
                product_type = data['hits'][i]['product_type']
            except:
                product_type = 'error'
                
            status = data['hits'][i]['in_stock']
            quantity = data['hits'][i]['quantity']
            try:
                presentation = data['hits'][i]['presentation']
            except:
                presentation = 'error'
            
            try:
                master_product_id = data['hits'][i]['master_product_id']
            except:
                master_product_id = 0000000
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            data_in = {'store_id': store_id,'product_id': product_id, 'ean': ean, 'product_name': name, 'price': price,
                       'description': description, 'product_type': product_type, 'presentation': presentation, 'status': status,
                       'quantity': quantity, 'master_product_id': master_product_id, 'create_date': date}
            result.append(data_in)
    else:
        print('error')
    
    return 
    
    
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema_mx'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_CONVENIENCE_SEARCH['query_stores']).fetchall()
stores_len = len(result_stores)

for i in range(0, stores_len):
    print(result_stores[i])
    get_product(int(result_stores[i]['store_id']), str(result_stores[i]['store_type']))

#get_product(581, 'ok_market_big')



df = pd.DataFrame(result)    

df.head(0).to_sql(config.RAPPI_CONVENIENCE_SEARCH['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_CONVENIENCE_SEARCH['table_lz'], null="") # null values become ''
conn.commit()

connection.close()

