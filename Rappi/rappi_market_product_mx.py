# -*- coding: utf-8 -*-
import pandas as pd
import config
import threading
import json
import io
import geo
import requests
import threading 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

result = []

def call_product(offset, stores_len_in):
    for i in range(0, stores_len_in):
        get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), offset)
    return

def get_product(store_id, aisle_id, offset):
    
    #{"state": {"aisle_id": "21111", "parent_id": "21111"}, "limit": 10, "offset": 0, "context": "sub_aisles", "stores": [store]}
    parameters = {"state": {"aisle_id": aisle_id}, "limit": 10000, "offset": offset, "context": "aisle_detail", "stores": [int(store_id)]}
    
    #parameters = {"state": {"lat": lat, "lng": lng, "parent_store_type": "market"}, "context": "store_home", "limit": 100,"stores": [int(store_id)]}
    print(parameters)
    url = 'https://services.mxgrability.rappi.com/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        #print('ok')
        data = json.loads(y.text)
        #print(data)
        
        components_len = len(data['data']['components'])
        for i in range(0, components_len):
            products_len = len(data['data']['components'][i]['resource']['products'])
            for j in range(0, products_len):
                product_id = data['data']['components'][i]['resource']['products'][j]['product_id']
                
                ean = data['data']['components'][i]['resource']['products'][j]['ean']
                name = data['data']['components'][i]['resource']['products'][j]['name']
                name = name.replace('\n', ' ')
                name = name.replace('\r', ' ')
                name = name.replace('\t', ' ')
                print(name)
                
                price = str(data['data']['components'][i]['resource']['products'][j]['price'])
                
                product_type = data['data']['components'][i]['resource']['products'][j]['product_type']
                product_type = product_type.replace('\n', ' ')
                product_type = product_type.replace('\r', ' ')
                product_type = product_type.replace('\t', ' ')
                
                presentation = data['data']['components'][i]['resource']['products'][j]['presentation']
                presentation = presentation.replace('\n', ' ')
                presentation = presentation.replace('\r', ' ')
                presentation = presentation.replace('\t', ' ')
                
                retail_id = data['data']['components'][i]['resource']['products'][j]['retail_id']
                
                trademark = data['data']['components'][i]['resource']['products'][j]['trademark']
                trademark = trademark.replace('\n', ' ')
                trademark = trademark.replace('\r', ' ')
                trademark = trademark.replace('\t', ' ')
                
                status = data['data']['components'][i]['resource']['products'][j]['in_stock']
                if status == False:
                    print(name)
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                data_in = {'product_id': product_id, 'store_id': store_id, 'ean': ean, 'product_name': name, 'price': price, 
                           'product_type': product_type, 'presentation': presentation, 'retail_id': retail_id, 'brand': trademark,'creation_date': date, 'status': status}
                result.append(data_in)
                
            
    else:
        print('error')
    
    return 
    
    
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema_mx'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_MARKET_PRODUCT['query_aisle_mx']).fetchall()
stores_len = len(result_stores)
'''
for i in range(0, stores_len):
    print(result_stores[i])
    get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), 0)
    get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), 10)
    get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), 20)
 '''
#get_product(990002972, '99716')




t1 = threading.Thread(name="hilo1", target=call_product, args=(0, stores_len))
t2 = threading.Thread(name="hilo2", target=call_product, args=(10, stores_len))
t3 = threading.Thread(name="hilo3", target=call_product, args=(20, stores_len))


#ejecuta los hilos
t1.start()
t2.start()
t3.start()

#espera a que terminen los hilos, para continuar
t1.join()
t2.join()
t3.join()


df = pd.DataFrame(result)    

df.head(0).to_sql(config.RAPPI_MARKET_PRODUCT['table_spot_mx'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_MARKET_PRODUCT['table_spot_mx'], null="") # null values become ''
conn.commit()

connection.close()

