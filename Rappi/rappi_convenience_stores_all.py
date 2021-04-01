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

id_vm = 2

def create_url(lat,lng):
    link = 'https://services.rappi.cl/api/sidekick/base-crack/principal?lng='+lng+'&lat='+lat
    #print(link)
    return link

def get_name(lat, lng, store_id, store_type):
    #tiendas de convivencia "parent_store_type": "express_big" para farmacia es farma
    parameters = {"state": {"lat": str(lat), "lng": str(lng), "parent_store_type": "express_big", "store_type": store_type}, "context": "store_home", "limit": 100,
    "stores": [int(store_id)]}
    #print(parameters)

    url = 'https://services.rappi.cl/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
    _name = ''        
    if y.status_code == 200:
        data = json.loads(y.text)
        _name = data['data']['context_info']['store']['name']

        print(_name)
    else:
        _name = 'error'
    return _name


def get_stores(lat_in, lng_in):
    url = create_url(lat_in, lng_in)
    response = requests.get(url)
    
    if response.status_code == 200:
        
        try:
            data = json.loads(response.text)
            market_len = len(data[4]['suboptions'])
    
            for i in range(0, market_len):
                print(i)
                market_name = data[2]['suboptions'][i]['name']
                #if market_name == 'Punto' or market_name == 'Upa!' or market_name == 'OK Market' or market_name == 'Petrobras' or market_name == 'Pronto' or market_name == 'Upita' or market_name == 'Oxxo':
                #    print(market_name+str(i))
                    
                stores_len = len(data[2]['suboptions'][i]['stores'])
                for j in range(0, stores_len):
                    store_id = data[2]['suboptions'][i]['stores'][j]['store_id']
                    brand_id = data[2]['suboptions'][i]['stores'][j]['brand_id']
                    lat = data[2]['suboptions'][i]['stores'][j]['lat']
                    lng = data[2]['suboptions'][i]['stores'][j]['lng']
                    zone_id = data[2]['suboptions'][i]['stores'][j]['zone_id']
                    layout_store_id = data[2]['suboptions'][i]['stores'][j]['layout_store_id']
                    store_type = data[2]['suboptions'][i]['stores'][j]['store_type']
                    try: 
                        name_in = get_name(lat, lng, store_id, store_type)
                    except:
                        name_in = 'error'  
                    
                    store_data = {'store_id': store_id, 'brand_id': brand_id, 'lat': lat, 'lng': lng, 'zone_id': zone_id,
                                      'layout_store_id': layout_store_id, 'store_type': store_type, 'name': str(name_in)}
                    result.append(store_data)
            insert_db(result)
        except:
            #print('error')
            pass
    return

def insert_db(def_in):
    df = pd.DataFrame(def_in)    
    
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    df.head(0).to_sql(config.RAPPI_STORES_CONVENIENCE['table_lz'], engine, if_exists='append',index=False)                   
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.RAPPI_STORES_CONVENIENCE['table_lz'], null="") # null values become ''
    conn.commit()
    connection.close()
    result = []
    return

def balance_cargas(cord):
    sublocal = []     
    #balance de carga en las VM
    num_vm = 2
    result_stores = cord
    num_local = len(cord)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = num_local
    #vm2 = (num_local//num_vm) * 2
    #vm3 = (num_local//num_vm) * 3
    #vm4 = (num_local//num_vm) * 4
    #vm5 = (num_local//num_vm) * 5
    #vm6 = num_local
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(result_stores[k])

    else:
        for k in range(vm1, vm2):
            sublocal.append(result_stores[k])
              
    return sublocal    
    

result = []
result_aisle = []

lat_in = '-33.35111199999999'
lng_in = '-70.510522'
#get_stores(lat_in, lng_in)

#temp = get_name('-33.413387811791','-70.580831476969','581','ok_market_big')
#print(temp)
#temp = get_name('900036305')
#print(temp)


#comunas = len(geo.GEOLOCATION['Metropolitana'])
#regiones = geo.GEOLOCATION

#get_stores(lat_in, lng_in) SOLO RM
#for i in range(0, comunas):
#    print(geo.GEOLOCATION['Metropolitana'][i]['name'])
#    get_stores(geo.GEOLOCATION['Metropolitana'][i]['lat'], geo.GEOLOCATION['Metropolitana'][i]['lng'])

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
connection = engine.connect()

coordenadas = connection.execute(config.GEO['query_geo']).fetchall()
subcord = balance_cargas(coordenadas)
print(len(subcord))
i = 0
for cord in subcord:
    i = i+1
    get_stores(str(cord['new_lat']), str(cord['new_lng']))
    




    

