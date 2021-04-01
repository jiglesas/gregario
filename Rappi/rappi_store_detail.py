# -*- coding: utf-8 -*-
from selenium import webdriver
from seleniumrequests import Chrome
from bs4 import BeautifulSoup
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

#api sample
#https://services.rappi.cl/api/sidekick/base-crack/principal?lng=-70.6211525&lat=-33.4588663
#https://services.rappi.cl/api/auth/guest_access_token
#https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/900024780

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
        print(wp_s[0])
        url = get_url(wp_s[0])
        hed = {'Authorization': 'Bearer ' + token}
        parameters = {"store_type": "restaurant", "lat": 0, "lng": 0}
        y = requests.post(url, headers=hed, json=parameters)
        
        if y.status_code == 200:
            #print('ok')
            data = json.loads(y.text)
        
            store_id = str(data['store_id'])
            super_store_id = str(data['super_store_id'])
            brand_name = str(data['brand_name'])
            store_type = str(data['store_type'])
            name = str(data['name'])
            address = data['address']
            address = address.replace('\n', ' ')
            address = address.replace('\t', ' ')
            print(address)
            zone_id = str(data['zone_id'])
            lat = str(data['location'][1])
            lng = str(data['location'][0])
            tag = str(data['tags'][0]['name'])
                
            data = {'store_id': store_id, 'super_store_id': super_store_id, 'brand_name': brand_name, 'store_type': store_type,
                    'name': name, 'address': address, 'zone_id': zone_id, 'lng': lng, 'lat': lat, 'tag': tag}
        
            result.append(data)
            time.sleep(1)
        else:
            print('error:'+wp_s[0])

    return
    

result = []    
token = get_token()
id_in = '363'

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_STORES_DETAIL['query_stores']).fetchall()
#for i in result_stores:
#    get_store_detail(str(i[0]))
#    print (i[0])




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
df.head(0).to_sql(config.RAPPI_STORES_DETAIL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_STORES_DETAIL['table_lz'], null="") # null values become ''
conn.commit()
connection.close()



    
