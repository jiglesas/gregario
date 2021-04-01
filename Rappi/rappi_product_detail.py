# -*- coding: utf-8 -*-
import time
import pandas as pd
from random import randrange
import config
import json
import threading
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

#funcion para crear el token (dura 1 hra el token)
def get_token():
    x = requests.post('https://services.rappi.cl/api/auth/guest_access_token')
    data = json.loads(x.text)
    token = data['access_token']
    return token

def get_url(id_):
    url = 'https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/'+id_
    return url

def get_store_detail(inicio, fin):
    local = []
    for i in range(inicio,fin):
        #print(result_stores[i])
        local.append(result_stores[i])
    
    for wp_s in local:
        #print(wp_s[0])
        url = get_url(wp_s[0])
        hed = {'Authorization': 'Bearer ' + token}
        parameters = {"store_type": "restaurant", "lat": 0, "lng": 0}
        y = requests.post(url, headers=hed, json=parameters)
        
        if y.status_code == 200:
            #print('ok')
            data = json.loads(y.text)
            #cantidad de pasillos
            corridors_count = len(data['corridors'])
            
            for i in range(0, corridors_count):
                
                products_count = len(data['corridors'][i]['products'])
                print(products_count)
                for j in range(0, products_count):
                    store_id = data['corridors'][i]['products'][j]['store_id']
                    store_name = data['brand_name']
                    product_id = data['corridors'][i]['products'][j]['product_id']
                    product_name = data['corridors'][i]['products'][j]['name']
                    product_name = product_name.replace('\n', ' ')
                    product_name = product_name.replace('\t', ' ')
                    corridor_id = data['corridors'][i]['products'][j]['corridor_id']
                    corridor_name = data['corridors'][i]['products'][j]['corridor_name']
                    corridor_name = corridor_name.replace('\n', ' ')
                    corridor_name = corridor_name.replace('\t', ' ')
                    description = data['corridors'][i]['products'][j]['description']
                    description = description.replace('\n', ' ')
                    description = description.replace('\t', ' ')
                    real_price = data['corridors'][i]['products'][j]['real_price']
                    price = data['corridors'][i]['products'][j]['price']
                
                    info = {'store_id': store_id, 'store_name': store_name, 'product_id': product_id, 'product_name': product_name, 'corridor_id': corridor_id,
                             'corridor_name': corridor_name, 'description': description, 'real_price': real_price, 'price': price}
                
                    result.append(info)
            time.sleep(1)
        else:
            print('error:'+wp_s[0])

    return
    

result = []    
token = get_token()

#conexion a la db
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_STORES_DETAIL['query_stores']).fetchall()


#ajuste de hilos y particion del los resultados obtenidos

len_stores = len(result_stores)
n_thread = 4
i0 = 0
i1 = len_stores//n_thread
i2 = (len_stores//n_thread) * 2
i3 = (len_stores//n_thread) * 3
i4 = len(result_stores)

tiempo_ini = datetime.now()

t1 = threading.Thread(name="hilo1", target=get_store_detail, args=(i0, i1))
t2 = threading.Thread(name="hilo2", target=get_store_detail, args=(i1, i2))
t3 = threading.Thread(name="hilo3", target=get_store_detail, args=(i2, i3))
t4 = threading.Thread(name="hilo4", target=get_store_detail, args=(i3, i4))


#ejecuta los hilos
t1.start()
t2.start()
t3.start()
t4.start()


#espera a que terminen los hilos, para continuar
t1.join()
t2.join()
t3.join()
t4.join()


df = pd.DataFrame(result)    

df.head(0).to_sql(config.RAPPI_PRODUCT_DETAIL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_PRODUCT_DETAIL['table_lz'], null="") # null values become ''
conn.commit()

connection.close()

