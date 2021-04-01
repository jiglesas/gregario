from bs4 import BeautifulSoup
import requests 
import pandas as pd
from selenium import webdriver
import time
import json
import config
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import io


def get_stores():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_stores = connection.execute(config.CORNERSHOP_AISLES['query_id_store']).fetchall()
    connection.close()
    return result_stores

def get_url(store_id):
    url = 'https://cornershopapp.com/api/v2/branches/'+str(store_id)+'?with_suspended_slots&country=CL'
    #print(url)
    return url

result = []
driver = webdriver.Chrome(ChromeDriverManager().install())
stores = get_stores()
for store in stores:
    #print(str(store))
    url = get_url(str(store[0]))
    print(url)
    #inicializacion del webdriver
    options = webdriver.ChromeOptions()
    #options.add_argument('--ignore-certificate-errors')
    #options.add_argument('--incognito')
    options.add_argument('start-maximized')
    #options.add_argument('--headless')
    #driver = webdriver.Chrome('./chromedriver.exe', options=options)
    
    driver.get(url)
    time.sleep(2)
    
    content_api = driver.page_source.encode("utf-8").strip()
    soup_api = BeautifulSoup(content_api, "html.parser")
    api_content = soup_api.find('pre').text
    data = json.loads(api_content)
    
    
    #se determina la cantidad de departamentos
    try:
        departments = len(data['departments'])
    except:
        departments = 0
    print(departments)

    
    #revisar opcion en dataframe***********************
    if departments > 0:
        for i in range(0, departments):
            #se determina la cantidad de pasillos
            aisles = len(data['departments'][i]['aisles'])
            for j in range(0, aisles):
                
                department_id = data['departments'][i]['id']
                
                department_name = data['departments'][i]['name']
                aisle_id = data['departments'][i]['aisles'][j]['id']
                aisle_name = data['departments'][i]['aisles'][j]['name']
                data_in = {'department_id': str(department_id), 'department_name': str(department_name), 'aisle_id': str(aisle_id), 'aisle_name': str(aisle_name)}
                result.append(data_in)
    




#conexion a la bd e insercion
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
connection = engine.connect()
        
df = pd.DataFrame(result)
df.head(0).to_sql(config.CORNERSHOP_AISLES['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents_img = output.getvalue()
cur.copy_from(output, config.CORNERSHOP_AISLES['table_lz'], null="") # null values become ''
conn.commit()
        
connection.close()
driver.quit()
