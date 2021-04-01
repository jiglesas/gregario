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

def get_stores(lat_in, lng_in):
    url = create_url(lat_in, lng_in)
    response = requests.get(url)
      
    if response.status_code == 200:
        data = json.loads(response.text)
        stores = len(data[0]['stores'])
        
        for i in range(0,stores):
            store_id = data[0]['stores'][i]['store_id']
            zone_id = data[0]['stores'][i]['zone_id']
            lat = data[0]['stores'][i]['lat']
            lng = data[0]['stores'][i]['lng']
            brand_id = data[0]['stores'][i]['brand_id']
            store_zone_id = data[0]['stores'][i]['store_zone_id']
            store_type = data[0]['stores'][i]['store_type']
            layout_store_id = 'error'
            store_data = {'store_id': store_id, 'zone_id': zone_id, 'lat': lat, 'lng': lng, 'brand_id': brand_id, 'store_zone_id': store_zone_id,
                     'layout_store_id': layout_store_id ,'store_type': store_type}
            result.append(store_data)
    return

result = []
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
lat_in = '-33.3500000'
lng_in = '-70.6666667'
#get_stores(lat_in, lng_in)

#todas las reegiones desde tabla
coordenadas = connection.execute(config.GEO['query_geo']).fetchall()
connection.close()
print(len(coordenadas))
for cord in coordenadas:
    get_stores(str(cord['new_lat']), str(cord['new_lng']))

'''
comunas = len(geo.GEOLOCATION['Metropolitana'])
for i in range(0, comunas):
    print(geo.GEOLOCATION['Metropolitana'][i]['name'])
    get_stores(geo.GEOLOCATION['Metropolitana'][i]['lat'], geo.GEOLOCATION['Metropolitana'][i]['lng'])
'''
    
df = pd.DataFrame(result)    
df.head(0).to_sql(config.RAPPI_STORES_ZONE['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_STORES_ZONE['table_lz'], null="") # null values become ''
conn.commit()

