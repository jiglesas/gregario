# -*- coding: utf-8 -*-
import time
from bs4 import BeautifulSoup
import pandas as pd
import config
from datetime import datetime
import io
import requests 
import json
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

def get_url():
    url = 'https://apijumboweb.smdigital.cl/cms/api/v1/json/cms/page-18721.json'
    return url

api_key = 'IuimuMneIKJd3tapno2Ag1c1WcAES97j'
url = get_url()
result = []

hed = {'x-api-key': api_key}
parameters = {"store_type": "restaurant", "lat": 0, "lng": 0}
y = requests.get(url, headers=hed)


print(y)

if y.status_code == 200:
    #print('ok')
    data = json.loads(y.text)
        
    len_store = len(data['acf']['store'])
    for i in range(0,len_store):
        store_id = data['acf']['store'][i]['sc']
        store_name = data['acf']['store'][i]['name']
        address = data['acf']['store'][i]['address']
        activated = False
        print(store_name)
        info = {'id': store_id, 'name': store_name, 'adress': address, 'activated': activated}
        result.append(info)
        
        
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
        
        
df = pd.DataFrame(result)
df = df.reindex(columns=['id','name', 'adress', 'activated'])

df.head(0).to_sql(config.JUMBO_STORES['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.JUMBO_STORES['table_lz'], null="") # null values become ''
conn.commit()
connection.close()
    