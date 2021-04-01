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
#https://services.rappi.cl/api/cpgs/global-search/search

def get_product(inicio, fin):
    local = []
    for i in range(inicio,fin):
        #print(result_stores[i])
        temp = ''.join(total[i])
        local.append(int(temp))

    #print(local)
    search_json = json.loads('{"store_ids": '+str(local)+' ,"keyword":"tika"}')
    
    parameters = search_json
    
    url = 'https://services.rappi.cl/api/cpgs/global-search/search'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        print('ok')
        data = json.loads(y.text)
        
        #print(len(data))
        for i in range(0, len(data)):
            store_name = data[i]['store_name']
            len_product = len(data[i]['products'])
            for j in range(0, len_product):
                store_id = data[i]['products'][j]['store_id']
                product_id = data[i]['products'][j]['product_id']
                product_name = data[i]['products'][j]['name']
                product_name = product_name.replace('\n', ' ')
                product_name = product_name.replace('\t', ' ')
                real_price = data[i]['products'][j]['real_price']
                presentation = data[i]['products'][j]['presentation']
                in_stock = data[i]['products'][j]['in_stock']
                quantity = data[i]['products'][j]['quantity']
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                data_product = {'product_id': product_id, 'store_id': store_id, 'product_name': product_name, 'store_name': store_name,
                                'real_price': real_price, 'presentation': presentation, 'status': in_stock, 'quantity': quantity, 'create_date': date}
                result.append(data_product)

    else:
        print('error')
    
    return 
    
    
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema_mx'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_CONVENIENCE_PRODUCT['query_stores_prod']).fetchall()
result_markets = connection.execute(config.RAPPI_STORES_MARKET['query_stores_prod']).fetchall()

connection.close()
temp_total = result_markets + result_stores
total_len = len(temp_total)
total = []

for rc in temp_total:
    var_temp = json.dumps(rc[0])
    categories_in = json.loads(var_temp)
    category_in = categories_in['category_id']
    store_in = categories_in['store_id']
    total.append(store_in)



#manejo de las tiendas (se divide en 15 porque es el maximo de stores que te responde la api a la vez)

split_stores = 15
divisor = total_len //split_stores
for z in range(0, divisor+1):
    
    k=15
    
    if z == 0:
        get_product(0,k)
    elif z < divisor:
        get_product(k*z, (k*z)+k)
    else:
        
        get_product(divisor*k, total_len)





df = pd.DataFrame(result)    

df.head(0).to_sql(config.RAPPI_CONVENIENCE_SEARCH_ALL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_CONVENIENCE_SEARCH_ALL['table_lz'], null="") # null values become ''
conn.commit()


