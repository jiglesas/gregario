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
    url = 'https://apis.santaisabel.cl:8443/catalog/api/v1/tresponiente/facets/search/busca?fq=H%3A193&nombre_promo=boton-ofertas-home08072019&sc=1'
    return url

def get_key():
    api_key = '5CIqbUOvJhdpZp4bIE5jpiuFY3kLdq2z'
    head = {'x-api-key': api_key}
    return head

def insert_db(informacion):
    df = pd.DataFrame(informacion)
    df = df.reindex(columns=['aisle_id','aisle_name','aisle_link','aisle_value','subaisle_id','subaisle_name','subaisle_link','subaisle_value', 'insert_date'])

    #conexion a la db
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

    df.head(0).to_sql(config.SISA_CATEGORY['table_lz'], engine, if_exists='append',index=False)
                
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.SISA_CATEGORY['table_lz'], null="") # null values become ''
    conn.commit()
    return 1
  
y = requests.get(get_url(), headers=get_key())
print(y)
if y.status_code == 200:
    #print('ok')
    data = json.loads(y.text)   
    for i in range(0,len(data['CategoriesTrees'])):
        aisle_id = data['CategoriesTrees'][i]['Id']
        aisle_name = data['CategoriesTrees'][i]['Name']
        aisle_link = data['CategoriesTrees'][i]['Link']
        aisle_value = data['CategoriesTrees'][i]['Value']
        print(aisle_name+'-------------')
        for j in range(0, len(data['CategoriesTrees'][i]['Children'])):
            subaisle_id = data['CategoriesTrees'][i]['Children'][j]['Id']
            subaisle_name = data['CategoriesTrees'][i]['Children'][j]['Name']
            print(subaisle_name)
            subaisle_link = data['CategoriesTrees'][i]['Children'][j]['Link']
            subaisle_value = data['CategoriesTrees'][i]['Children'][j]['Value']
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            info = {'aisle_id': aisle_id, 'aisle_name': aisle_name, 'aisle_link': aisle_link, 'aisle_value': aisle_value,
                    'subaisle_id': subaisle_id, 'subaisle_name': subaisle_name, 'subaisle_link': subaisle_link, 'subaisle_value': subaisle_value
                    ,'insert_date': date}
            result.append(info)
     
temp = insert_db(result)
if temp == 1: 
    print('exito')


