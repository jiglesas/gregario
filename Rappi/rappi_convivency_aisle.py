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

result = []

def get_aisle(store_id):
    parameters = {"state": {}, "limit": 100, "offset": 0, "context": "aisles_tree", "stores": [int(store_id)]}
    
    #parameters = {"state": {"lat": lat, "lng": lng, "parent_store_type": "market"}, "context": "store_home", "limit": 100,"stores": [int(store_id)]}
    print(store_id)
    url = 'https://services.rappi.cl/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        data = json.loads(y.text)
        
        components_len = len(data['data']['components'])
        for i in range(0, components_len):
            category_len = len(data['data']['components'][i]['resource']['categories'])
            for j in range(0, category_len):
                _id = data['data']['components'][i]['resource']['categories'][j]['id']
                name = data['data']['components'][i]['resource']['categories'][j]['name']
                
                data_in = {'store_id': store_id, 'category_id': _id, 'category_name': name}
                result.append(data_in)
    else:
        print('error')
    
    return 
    
    
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_STORES_CONVENIENCE['query_stores']).fetchall()
for wp in result_stores:
    get_aisle(wp[0])


df = pd.DataFrame(result)    
df.head(0).to_sql(config.RAPPI_CONVENIENCE_AISLE['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_CONVENIENCE_AISLE['table_lz'], null="") # null values become ''
conn.commit()

connection.close()
