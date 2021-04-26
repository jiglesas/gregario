# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
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

local = []
result = []
id_vm = 2
#funcion para crear el token (dura 1 hra el token)
def get_token():
    x = requests.post('https://services.rappi.cl/api/auth/guest_access_token')
    data = json.loads(x.text)
    token = data['access_token']
    print(token)
    return token

def get_url(id_):
    url = 'https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/'+id_
    return url

def balance_cargas():
    sublocal = []     
    #balance de carga en las VM, se distribuyen la cantidad de locales entrantes 
    num_vm = 6
    result_stores = get_restaurant()
    num_local = len(result_stores)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = (num_local//num_vm) * 4
    vm5 = (num_local//num_vm) * 5
    vm6 = num_local
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(result_stores[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(result_stores[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(result_stores[k])
    elif id_vm == 4:
        for k in range(vm3, vm4):
            sublocal.append(result_stores[k])
    elif id_vm == 5:
        for k in range(vm4, vm5):
            sublocal.append(result_stores[k])
    else:
        for k in range(vm5, vm6):
            sublocal.append(result_stores[k])
              
    return sublocal

def insert_db(def_in):
    df = pd.DataFrame(def_in)    
    
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

    df.head(0).to_sql(config.RAPPI_RESTAURANT_PRODUCT['table_lz'], engine, if_exists='append',index=False)                   
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.RAPPI_RESTAURANT_PRODUCT['table_lz'], null="") # null values become ''
    conn.commit()
    result = []
    return

def get_restaurant_product(_id, lat, lng):
    url = get_url(_id)
    hed = {'Authorization': 'Bearer ' + token}
    parameters = {"store_type": "restaurant", "lat": lat, "lng": lng}
    y = requests.post(url, headers=hed, json=parameters)

    if y.status_code == 200:
        data = json.loads(y.text)
        
        store_id = _id
        super_store_id = str(data['super_store_id'])
        
        #print(len(data['corridors']))
        
        for j in range(0, len(data['corridors'])):
            #print(len(data['corridors'][j]['products']))
            for k in range(0, len(data['corridors'][j]['products'])):
                try:
                    product_id = data['corridors'][j]['products'][k]['product_id']
                    store_id = data['corridors'][j]['products'][k]['store_id']
                    name = data['corridors'][j]['products'][k]['name']
                    name = name.replace('\n', ' ')
                    name = name.replace('\t', ' ')
                    description = data['corridors'][j]['products'][k]['description']
                    description = description.replace('\n', ' ')
                    description = description.replace('\t', ' ')
                    corridor_id = data['corridors'][j]['products'][k]['corridor_id']
                    corridor_name = data['corridors'][j]['products'][k]['corridor_name']
                    corridor_name = corridor_name.replace('\n', ' ')
                    corridor_name = corridor_name.replace('\t', ' ')
                    price = data['corridors'][j]['products'][k]['price']
                    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    popup = get_popup(store_id, product_id)
                except:
                    pass
                try:
                    data_in = {'product_id': str(product_id), 'store_id': str(store_id), 'product_name': str(name), 'description': str(description), 
                            'corridor_id': str(corridor_id), 'corridor_name': str(corridor_name), 'price': str(price), 'create_date': str(date), 'popup': str(popup)}
                    
                    result.append(data_in)
                except:
                    pass
        
        try:
            insert_db(result)
        except:
            pass
        time.sleep(1)
        #result=[]
    else:
        print('error:'+_id)

    return
    
def get_restaurant():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()

    restaurants = connection.execute(config.RAPPI_RESTAURANT_PRODUCT['query_stores']).fetchall()
    connection.close()
    return restaurants

def get_popup(store, product):
    url = 'https://services.rappi.cl/api/restaurant-bus/products/toppings/'+str(store)+'/'+str(product)+'/v2'
    hed = {'Authorization': 'Bearer ' + token}
    y = requests.get(url, headers=hed)

    if y.status_code == 200:
        #print(y)
        data_pop = json.loads(y.text)
        pop_up = str(data_pop['categories'])
    return pop_up
    


##### MAIN ######
token = get_token()
#print(token)
result_stores = balance_cargas()

num = 0
#print(type(result_stores))
for rc in result_stores:
    print(str(num)+' de '+str(len(result_stores)))
    #print(rc['store_id'])
    get_restaurant_product(rc['store_id'], rc['lat'], rc['lng'])
    result=[]
    num = num+1
    