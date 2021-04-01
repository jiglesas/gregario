# -*- coding: utf-8 -*-
import pandas as pd
import config
import threading
import json
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import log


def insert_db(informacion):
    df = pd.DataFrame(informacion)
    #conexion a la db
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    #orden de cabeceras
    df = df.reindex(columns=['product_id','store_id','aisle_id','ean','product_name','price','product_type','presentation','retail_id','brand','creation_date', 'status'])
    df.head(0).to_sql(config.RAPPI_CONVENIENCE_PRODUCT['table_lz'], engine, if_exists='append',index=False)
                        
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.RAPPI_CONVENIENCE_PRODUCT['table_lz'], null="") # null values become ''
    conn.commit()
    return

def get_product(store_id, aisle_id, offset):
    result = []
    #{"state": {"aisle_id": "21111", "parent_id": "21111"}, "limit": 10, "offset": 0, "context": "sub_aisles", "stores": [store]}
    parameters = {"state": {"aisle_id": aisle_id}, "limit": 100, "offset": offset, "context": "aisle_detail", "stores": [int(store_id)]}
    
    #parameters = {"state": {"lat": lat, "lng": lng, "parent_store_type": "market"}, "context": "store_home", "limit": 100,"stores": [int(store_id)]}

    url = 'https://services.rappi.cl/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        #print('ok')
        data = json.loads(y.text)
        
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
                trademark = data['data']['components'][i]['resource']['products'][j]['trademark']
                trademark = trademark.replace('\n', ' ')
                trademark = trademark.replace('\t', ' ')
                status = data['data']['components'][i]['resource']['products'][j]['in_stock']
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                data_in = {'product_id': product_id, 'store_id': store_id, 'aisle_id': aisle_id, 'ean': ean, 'product_name': name, 'price': int(price), 
                           'product_type': product_type, 'presentation': presentation, 'retail_id': retail_id, 'brand': trademark,'creation_date': date, 'status': status}
                result.append(data_in)
        insert_db(result)
        result = []
    else:
        #print('error: '+str(parameters))
        pass
    
    return 

#encabezado log
print('Log Rappi Convenience')
print('HORA INICIO: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('Start', 'Rappi - Convenience VM')
       
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
result_stores = connection.execute(config.RAPPI_CONVENIENCE_PRODUCT['query_stores_prod']).fetchall()
connection.close()
#print(result_stores)
stores_len = len(result_stores)
i = 0   

    #t2.join()
for i in range(0, stores_len):
    #print(result_stores[i]['category_id'])
    get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), 0 )
    
print('HORA FIN: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('End', 'Rappi - Convenience VM')
 


#get_product(900019081, '98517', 0)

#df = pd.DataFrame(result)
#df = df.reindex(columns=['product_id','store_id','aisle_id','ean','product_name','price','product_type','presentation','retail_id','brand','creation_date', 'status'])

#df.head(0).to_sql(config.RAPPI_CONVENIENCE_PRODUCT['table_lz'], engine, if_exists='append',index=False)
                    
#conn = engine.raw_connection()
#cur = conn.cursor()
#output = io.StringIO()
#df.to_csv(output, sep='\t', header=False, index=False)
#output.seek(0)
#cur.copy_from(output, config.RAPPI_CONVENIENCE_PRODUCT['table_lz'], null="") # null values become ''
#conn.commit()


