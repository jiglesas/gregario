from bs4 import BeautifulSoup
import requests
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium import webdriver
import time
import json
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io

result = []
#inicializacion del webdriver
api_url = 'https://cornershopapp.com/api/v2/branches/664?with_suspended_slots&locality=Quilicura&country=CL'
options = webdriver.ChromeOptions()
#options.add_argument('--ignore-certificate-errors')
#options.add_argument('--incognito')
options.add_argument('start-maximized')
#options.add_argument('--headless')
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get(api_url)
time.sleep(1)

content_api = driver.page_source.encode("utf-8").strip()
soup_api = BeautifulSoup(content_api, "html.parser")
api_content = soup_api.find('pre').text
data = json.loads(api_content)

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema2'])})

connection = engine.connect()

#name = data['departments'][0]['aisles'][1]
#se determina la cantidad de departamentos
departments = len(data['departments'])

#revisar opcion en dataframe***********************
for i in range(0, departments):
    #se determina la cantidad de pasillos
    aisles = len(data['departments'][i]['aisles'])
    for j in range(0, aisles):
        department_id = data['departments'][i]['id']
        department_name = data['departments'][i]['name']
        print(department_name)
        aisle_id = data['departments'][i]['aisles'][j]['id']
        aisle_name = data['departments'][i]['aisles'][j]['name']
        
        data = {'department_id':department_id, 'department_name':department_name, 'aisle_id':aisle_id, 'aisle_name': aisle_name}
        result.append(data)        


df = pd.DataFrame(result)
df.head(0).to_sql(config.CORNERSHOP_AISLES['table_spot'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents_img = output.getvalue()
cur.copy_from(output, config.CORNERSHOP_AISLES['table_spot'], null="") # null values become ''
conn.commit()
        
connection.close()
driver.quit()
