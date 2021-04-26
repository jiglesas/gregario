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

result = []

def get_url():
    url = 'https://apis.santaisabel.cl:8443/sisa/json/cms/page-560.json'
    return url

def get_key():
    api_key = '5CIqbUOvJhdpZp4bIE5jpiuFY3kLdq2z'
    head = {'x-api-key': api_key}
    return head
  
#parameters = {"store_type": "restaurant", "lat": 0, "lng": 0}
y = requests.get(get_url(), headers=get_key())

if y.status_code == 200:
    #print('ok')
    data = json.loads(y.text)
        
    len_store = len(data['acf']['store'])
    for i in range(0,len_store):
        store_id = data['acf']['store'][i]['sc']
        store_name = data['acf']['store'][i]['name']
        warehouse_id = data['acf']['store'][i]['warehouse_id']
        address = data['acf']['store'][i]['address']
        seller = data['acf']['store'][i]['seller']
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        info = {'id': store_id, 'name': store_name, 'adress': address, 'seller': seller, 'warehouse_id': warehouse_id, 'date': date}
        result.append(info)
        
        
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})    
        
df = pd.DataFrame(result)
df = df.reindex(columns=['id','name', 'adress', 'seller', 'warehouse_id', 'date'])

df.head(0).to_sql(config.SISA_STORES['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.SISA_STORES['table_lz'], null="") # null values become ''
conn.commit()
