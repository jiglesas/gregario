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

#https://services.mxgrability.rappi.com/api/sidekick/base-crack/principal?lng=-99.133208&lat=19.4326077

def create_url(lat,lng):
    link = 'https://services.mxgrability.rappi.com/api/sidekick/base-crack/principal?lng='+lng+'&lat='+lat
    return link

def get_name(lat, lng, store_id):
    parameters = {"state": {"lat": str(lat), "lng": str(lng), "parent_store_type": "market"}, "context": "store_home", "limit": 100,
    "stores": [int(store_id)]}
    print(parameters)

    url = 'https://services.mxgrability.rappi.com/api/dynamic/context/content'
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
        market_len = len(data[1]['suboptions'])

        for i in range(0, market_len):
            market_name = data[1]['suboptions'][i]['name']
            if market_name == 'Walmart' or market_name == 'Soriana' or market_name == 'Superama' or market_name == 'Chedraui' or market_name == 'La Comer' or market_name == 'Chedraui Express' or market_name == 'Caja Rápida La Comer':
                print(market_name+str(i))
                
                stores_len = len(data[1]['suboptions'][i]['stores'])
                for j in range(0, stores_len):
                    store_id = data[1]['suboptions'][i]['stores'][j]['store_id']
                    brand_id = data[1]['suboptions'][i]['stores'][j]['brand_id']
                    lat = data[1]['suboptions'][i]['stores'][j]['lat']
                    lng = data[1]['suboptions'][i]['stores'][j]['lng']
                    zone_id = data[1]['suboptions'][i]['stores'][j]['zone_id']
                    layout_store_id = data[1]['suboptions'][i]['stores'][j]['layout_store_id']
                    store_type = data[1]['suboptions'][i]['stores'][j]['store_type']
                    name_in = get_name(lat, lng, store_id)
                    
                    store_data = {'store_id': store_id, 'brand_id': brand_id, 'lat': lat, 'lng': lng, 'zone_id': zone_id,
                                  'layout_store_id': layout_store_id, 'store_type': store_type, 'name': name_in}
                    result.append(store_data)


    return

result = []

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema_mx'])})

connection = engine.connect()

result_geolocation = connection.execute(config.GEO_MX['query_geo']).fetchall()
len_geo = len(result_geolocation)

for i in range(0, len_geo):
    print(result_geolocation[i]['lat'])
    get_stores(str(result_geolocation[i]['lat']), str(result_geolocation[i]['lng']))
    


    
df = pd.DataFrame(result)  
  
df.head(0).to_sql(config.RAPPI_STORES_MARKET_MX['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_STORES_MARKET_MX['table_lz'], null="") # null values become ''
conn.commit()
connection.close()

