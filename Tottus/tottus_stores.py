import pandas as pd
import config
import threading
import json
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

result = []

def get_url():
    link = 'https://www.tottus.cl/api/contact-points?perPage=500'
    return link

def get_stores():
    url = get_url()
    response = requests.get(url)
    
    if response.status_code == 200:
        data = json.loads(response.text)
        
        stores_len = len(data['results'])
        for i in range(0, stores_len):
            store_id = data['results'][i]['id']
            store_name = data['results'][i]['name']
            region = data['results'][i]['address']['region']
            province = data['results'][i]['address']['province']
            district = data['results'][i]['address']['district']
            address = data['results'][i]['address']['streetName']
            geocode = data['results'][i]['address']['geoCode']
            
            store_data = {'store_id': store_id, 'store_name': store_name, 'region': region, 'province': province, 'district': district,
                           'address': address, 'geocode': geocode}
            result.append(store_data)
    return

get_stores()
df = pd.DataFrame(result)           
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()      
 
df.head(0).to_sql(config.TOTTUS_STORES['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.TOTTUS_STORES['table_lz'], null="") # null values become ''
conn.commit()
connection.close()