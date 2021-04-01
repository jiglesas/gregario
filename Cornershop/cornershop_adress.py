from bs4 import BeautifulSoup
import requests 
import pandas as pd
from selenium import webdriver
import time
import json
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io

#api sample
#https://cornershopapp.com/api/v2/branches/512?with_suspended_slots&locality=Talcahuano&country=CL
result_stores = []
result_toadd = []
#inicializacion del webdriver
options = webdriver.ChromeOptions()
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)


engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.CORNERSHOP_ADRESS['query_adress']).fetchall()


for wp_s in result_stores:
    api_url = 'https://cornershopapp.com/api/v2/branches/'+wp_s[0]
    print (api_url)
    driver.get(api_url)
    
    content_api = driver.page_source.encode("utf-8").strip()
    soup_api = BeautifulSoup(content_api, "html.parser")
    api_content = soup_api.find('pre').text
    data = json.loads(api_content)
    time.sleep(1)

    store_id = data['branch']['id']
    store_name = data['branch']['name']
    adress = data['branch']['address']
    organization_id = data['branch']['store_id']
    organization_name = data['branch']['store_name']
    country = data['branch']['country']
    
    info = {'store_id': store_id, 'organization_id': organization_id, 'organization_name': organization_name, 'store_name': store_name, 'adress': adress, 'country': country}
    result_toadd.append(info)
    
df = pd.DataFrame(result_toadd)
df.head(0).to_sql(config.CORNERSHOP_ADRESS['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.CORNERSHOP_ADRESS['table_lz'], null="") # null values become ''
conn.commit()
    
connection.close()
driver.close()
    
    