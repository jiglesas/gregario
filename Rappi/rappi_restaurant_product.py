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
#funcion para crear el token (dura 1 hra el token)
def get_token():
    x = requests.post('https://services.rappi.cl/api/auth/guest_access_token')
    data = json.loads(x.text)
    token = data['access_token']
    return token

def get_url(id_):
    url = 'https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/'+id_
    return url

def get_restaurant_product(_id, lat, lng):
    url = get_url(_id)
    hed = {'Authorization': 'Bearer ' + token}
    parameters = {"store_type": "restaurant", "lat": lat, "lng": lng}
    y = requests.post(url, headers=hed, json=parameters)
        
    if y.status_code == 200:
            #print('ok')
        data = json.loads(y.text)
        
        store_id = _id
        super_store_id = str(data['super_store_id'])
        
        for j in range(0, len(data['corridors'])):
            #print(len(data['corridors'][j]['products']))
            for k in range(0, len(data['corridors'][j]['products'])):
                product_id = data['corridors'][j]['products'][k]['product_id']
                store_id = data['corridors'][j]['products'][k]['store_id']
                name = data['corridors'][j]['products'][k]['name']
                description = data['corridors'][j]['products'][k]['description']
                corridor_id = data['corridors'][j]['products'][k]['corridor_id']
                corridor_name = data['corridors'][j]['products'][k]['corridor_name']
                price = data['corridors'][j]['products'][k]['price']
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                
                data = {'product_id': product_id, 'store_id': store_id, 'product_name': name, 'description': description, 
                        'corridor_id': corridor_id, 'corridor_name': corridor_name, 'price': price, 'create_date': date}
                

        

       
        
        result.append(data)
        time.sleep(1)
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


##### MAIN ######
token = get_token()
result_stores = get_restaurant()

print(type(result_stores))
#for rc in result_stores:
    #print(rc['store_id'])
#    get_restaurant_product(rc['store_id'], rc['lat'], rc['lng'])
    
    