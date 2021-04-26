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

def get_name(lat, lng, store_id):
    parameters = {"state": {"lat": str(lat), "lng": str(lng), "parent_store_type": "market"}, "context": "store_home", "limit": 100,
    "stores": [int(store_id)]}
    print(parameters)

    url = 'https://services.rappi.cl/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        data = json.loads(y.text)
        name = data['data']['context_info']['store']['name']
    else:
        name = 'error'
    return name

def get_stores(lat_in, lng_in):
    url = create_url(lat_in, lng_in)
    response = requests.get(url)
      
    if response.status_code == 200:
        data = json.loads(response.text)
        try:
            market_len = len(data[2]['suboptions'])
    
            for i in range(0, market_len):
                market_name = data[2]['suboptions'][i]['name']
                #if market_name == 'Lider' or market_name == 'Jumbo' or market_name == 'Unimarc' or market_name == 'Tottus':
                    #print(market_name+str(i))
                    
                stores_len = len(data[2]['suboptions'][i]['stores'])
                for j in range(0, stores_len):
                    store_id = data[2]['suboptions'][i]['stores'][j]['store_id']
                    brand_id = data[2]['suboptions'][i]['stores'][j]['brand_id']
                    lat = data[2]['suboptions'][i]['stores'][j]['lat']
                    lng = data[2]['suboptions'][i]['stores'][j]['lng']
                    layout_store_id = data[2]['suboptions'][i]['stores'][j]['layout_store_id']
                    store_type = data[2]['suboptions'][i]['stores'][j]['store_type']
                    name_in = get_name(lat, lng, store_id)
                        
                    store_data = {'store_id': store_id, 'brand_id': brand_id, 'lat': lat, 'lng': lng,
                                      'layout_store_id': layout_store_id, 'store_type': store_type, 'name': name_in}
                    result.append(store_data)
                insert_db(result)
        except:
            print('error')

    return

def insert_db(info_in):
    df = pd.DataFrame(info_in)    
    df.head(0).to_sql(config.RAPPI_STORES_MARKET['table_lz'], engine, if_exists='append',index=False)
                        
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.RAPPI_STORES_MARKET['table_lz'], null="") # null values become ''
    conn.commit()
    result=[]
    return
    

result = []
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
connection = engine.connect()

#todas las reegiones desde tabla
coordenadas = connection.execute(config.GEO['query_geo']).fetchall()
connection.close()
print(len(coordenadas))
for cord in coordenadas:
    get_stores(str(cord['new_lat']), str(cord['new_lng']))
    
#todas las regiones
#regiones = geo.GEOLOCATION
#for region in regiones:
#    print(region)
#    #print(len(regiones[region]))
#    comunas = regiones[region]
#    for comuna in comunas:
#        print(comuna['lat'])
#        get_stores(comuna['lat'], comuna['lng'])


    

