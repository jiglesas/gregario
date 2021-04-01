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
    link = 'https://www.tottus.cl/api/categories'
    return link

def get_stores():
    url = get_url()
    response = requests.get(url)
    
    if response.status_code == 200:
        data = json.loads(response.text)
        
        category_len = len(data)
        #print(category_len)
        for i in range(0, category_len):
            children_len = len(data[i]['children'])
            for j in range(0, children_len):
                subchildren_len = len(data[i]['children'][j]['children'])
                for k in range(0, subchildren_len):
                    #print(data[i]['children'][j]['children'][k]['slug'])
                    family_id = data[i]['id']
                    family_key = data[i]['key']
                    family_name = data[i]['name']
                    family_slug = data[i]['slug']
                    
                    category_id = data[i]['children'][j]['id']
                    category_key = data[i]['children'][j]['key']
                    category_name = data[i]['children'][j]['name']
                    category_slug = data[i]['children'][j]['slug']
                    
                    subcategory_id = data[i]['children'][j]['children'][k]['id']
                    subcategory_key = data[i]['children'][j]['children'][k]['key']
                    subcategory_name = data[i]['children'][j]['children'][k]['name']
                    subcategory_slug = data[i]['children'][j]['children'][k]['slug']
                    
                    category_data = {'family_id': family_id, 'family_key': family_key, 'family_name': family_name, 'family_slug': family_slug,
                                      'category_id': category_id, 'category_key': category_key, 'category_name': category_name, 'category_slug': category_slug,
                                       'subcategory_id': subcategory_id, 'subcategory_key': subcategory_key, 'subcategory_name': subcategory_name, 'subcategory_slug': subcategory_slug}
                    result.append(category_data)
    return

get_stores()
df = pd.DataFrame(result)    
      
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()      
 
df.head(0).to_sql(config.TOTTUS_CATEGORY['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.TOTTUS_CATEGORY['table_lz'], null="") # null values become ''
conn.commit()
connection.close()
