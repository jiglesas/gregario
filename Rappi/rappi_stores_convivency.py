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

#https://services.rappi.cl/api/sidekick/base-crack/principal?lng=-70.6211525&lat=-33.4588663

def create_url(lat,lng):
    link = 'https://services.rappi.cl/api/sidekick/base-crack/principal?lng='+lng+'&lat='+lat
    return link

def get_name(lat, lng, store_id, store_type):
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
            market_len = len(data[3]['suboptions'])
    
            for i in range(0, market_len):
                market_name = data[3]['suboptions'][i]['name']
                if market_name == 'Punto' or market_name == 'Upa!' or market_name == 'OK Market' or market_name == 'Petrobras' or market_name == 'Pronto' or market_name == 'Upita' or market_name == 'Oxxo':
                    print(market_name+str(i))
                    
                    stores_len = len(data[3]['suboptions'][i]['stores'])
                    for j in range(0, stores_len):
                        store_id = data[3]['suboptions'][i]['stores'][j]['store_id']
                        brand_id = data[3]['suboptions'][i]['stores'][j]['brand_id']
                        lat = data[3]['suboptions'][i]['stores'][j]['lat']
                        lng = data[3]['suboptions'][i]['stores'][j]['lng']
                        zone_id = data[3]['suboptions'][i]['stores'][j]['zone_id']
                        layout_store_id = data[3]['suboptions'][i]['stores'][j]['layout_store_id']
                        store_type = data[3]['suboptions'][i]['stores'][j]['store_type']
                        try: 
                            name_in = get_name(lat, lng, store_id, store_type)
                        except:
                            name_in = 'error'                    
                        store_data = {'store_id': store_id, 'brand_id': brand_id, 'lat': lat, 'lng': lng, 'zone_id': zone_id,
                                      'layout_store_id': layout_store_id, 'store_type': store_type, 'name': str(name_in)}
                        result.append(store_data)
        except:
            print('error')
    return

result = []
result_aisle = []

lat_in = '-33.4087844'
lng_in = '-70.567069'
#get_stores(lat_in, lng_in)

#comunas = len(geo.GEOLOCATION['Metropolitana'])
regiones = geo.GEOLOCATION

#get_stores(lat_in, lng_in) SOLO RM
#for i in range(0, comunas):
#    print(geo.GEOLOCATION['Metropolitana'][i]['name'])
#    get_stores(geo.GEOLOCATION['Metropolitana'][i]['lat'], geo.GEOLOCATION['Metropolitana'][i]['lng'])
    
#todas las regiones
for region in regiones:
    print(region)
    #print(len(regiones[region]))
    comunas = regiones[region]
    for comuna in comunas:
        print(comuna['lat'])
        get_stores(comuna['lat'], comuna['lng'])
    

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
    
df = pd.DataFrame(result)    
df.head(0).to_sql(config.RAPPI_STORES_CONVENIENCE['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_STORES_CONVENIENCE['table_lz'], null="") # null values become ''
conn.commit()
connection.close()


