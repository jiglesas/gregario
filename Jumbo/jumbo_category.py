# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests 
import pandas as pd
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io
import json
import config

API_KEY = 'IuimuMneIKJd3tapno2Ag1c1WcAES97j'

def get_url():
    url = 'https://apijumboweb.smdigital.cl/cms/api/v1/json/cms/page-11732.json'
    return url

def insert_db(data_in):                       
    df = pd.DataFrame(data_in)    
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    
    df = df.reindex(columns=['category_url','category_name', 'subcategory_url','subcategory_name'])
    df.head(0).to_sql(config.JUMBO_CATEGORY['table_lz'], engine, if_exists='append',index=False)
                    
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.JUMBO_CATEGORY['table_lz'], null="") # null values become ''
    conn.commit()
    return

def main():
    hed = {'x-api-key': API_KEY}
    y = requests.get(get_url(), headers=hed)
    result = []
    print(y)
    if y.status_code == 200:
        data = json.loads(y.text)
        
        for i in range(0, len(data['acf']['items'])):
                    
            category_url = data['acf']['items'][i]['url']
            category_name = data['acf']['items'][i]['title']
        
            for j in range(0, len(data['acf']['items'][i]['items'])):
                
                
                if data['acf']['items'][i]['items'][j]['items'] != False:
                    for k in range(0, len(data['acf']['items'][i]['items'][j]['items'])):
                        subcategory_url = data['acf']['items'][i]['items'][j]['items'][k]['url']
                        subcategory_name = data['acf']['items'][i]['items'][j]['items'][k]['title']
                        
                        info = {'category_url': category_url, 'category_name': category_name,
                                'subcategory_url': subcategory_url, 'subcategory_name': subcategory_name}
                        result.append(info)
                else:
                    subcategory_url = data['acf']['items'][i]['items'][j]['url']
                    subcategory_name = data['acf']['items'][i]['items'][j]['title']
                        
                    info = {'category_url': category_url, 'category_name': category_name,
                            'subcategory_url': subcategory_url, 'subcategory_name': subcategory_name}
                    result.append(info)
                
        insert_db(result)
    
if __name__ == "__main__":
    main()