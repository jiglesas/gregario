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
    #parametros mexico
    parameters = {"state": {}, "limit": 1000, "offset": 0, "context": "aisles_tree", "stores": [int(store_id)]}
    #parametros chile
    #parameters = {"state": {"lat": lat, "lng": lng, "parent_store_type": "market"}, "context": "store_home", "limit": 100,"stores": [int(store_id)]}

    #chile o mexico
    url = 'https://services.rappi.cl/api/dynamic/context/content'
    #url = 'https://services.mxgrability.rappi.com/api/dynamic/context/content'
    y = requests.post(url, json=parameters)
            
    if y.status_code == 200:
        data = json.loads(y.text)
        
        components_len = len(data['data']['components'])
        for i in range(0, components_len):
            category_len = len(data['data']['components'][i]['resource']['categories'])
            for j in range(0, category_len):
                _id = data['data']['components'][i]['resource']['categories'][j]['id']
                name = data['data']['components'][i]['resource']['categories'][j]['name']
                name = name.replace('\n', ' ')
                name = name.replace('\t', ' ')
                parent_id = data['data']['components'][i]['resource']['categories'][j]['parent_id']
                parent_name = data['data']['components'][i]['resource']['name']
                parent_name = parent_name.replace('\t', ' ')
                parent_name = parent_name.replace('\n', ' ')
                
                data_in = {'store_id': store_id, 'parent_id': parent_id, 'parent_name': parent_name, 'category_id': _id, 'category_name': name}
                result.append(data_in)
    else:
        print('error')
    
    return 
    
    
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.RAPPI_MARKET_AISLE['query_stores']).fetchall()
for wp in result_stores:
    get_aisle(wp[0])


df = pd.DataFrame(result)    
df.head(0).to_sql(config.RAPPI_MARKET_AISLE['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.RAPPI_MARKET_AISLE['table_lz'], null="") # null values become ''
conn.commit()

connection.close()