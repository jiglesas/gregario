# -*- coding: utf-8 -*-
import json
import urllib
import requests
import config
import pandas as pd
import config
import threading
import json
import io
import geo
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event


results = []
url = 'https://parseapi.back4app.com/classes/City?limit=10000000'
headers = {
    'X-Parse-Application-Id': 'XBDCWdxtLIjvbXp1pR4HZ3qPKDfqgt2OyEn8Wo8x', # This is the fake app's application id
    'X-Parse-Master-Key': 'oGuexYgobvHBECTDeE4icjmZR9Zd3YFX0JIapVnY' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
print(json.dumps(data, indent=2))

len_citys = len(data['results'])

for i in range(0, len_citys):
    #print(data['results'][i]['name'])
    object_id = data['results'][i]['objectId']
    city_id = data['results'][i]['cityId']
    name = data['results'][i]['name']
    lat = data['results'][i]['location']['latitude']
    lng =  data['results'][i]['location']['longitude']
    country = data['results'][i]['country']
    country_code = data['results'][i]['countryCode']
    adminCode = data['results'][i]['adminCode']
    
    result_data = {'object_id': object_id, 'city_id': city_id, 'name': name, 'lat': lat, 'lng': lng, 'country': country,
                   'country_code': country_code, 'admin_code': adminCode}

    results.append(result_data)
    
    

df = pd.DataFrame(results)           
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema_mx'])})

connection = engine.connect()      
 
df.head(0).to_sql(config.GEO_MX['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.GEO_MX['table_lz'], null="") # null values become ''
conn.commit()
connection.close()
    