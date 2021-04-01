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

#inicializacion del webdriver
url_root = 'https://cornershopapp.com/es-cl/'
options = webdriver.ChromeOptions()
#options.add_argument('--ignore-certificate-errors')
#options.add_argument('--incognito')
options.add_argument('start-maximized')
#options.add_argument('--headless')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get(url_root)
time.sleep(3)

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_data = []
content = driver.page_source.encode("utf-8").strip()
soup = BeautifulSoup(content, "lxml")
registro = {}

#capturar locales disponibles a nivel nacional
opciones = soup.findAll("option")

df_total = pd.DataFrame()
#se itera por cada local
for opc in opciones:
    #print (opc.text)
    #se quitan las regiones sin datos (Mejorar)
    if opc.text != 'Selecciona tu comuna' and opc.text != 'Pudahuel' and opc.text != 'Concón' and opc.text != 'Maitencillo' and opc.text != 'Penco' and opc.text != 'San Pedro de la Paz' and opc.text != 'Antofagasta' and opc.text != 'Rancagua' and opc.text != 'Temuco':
        opc_fix = opc.text.replace(' ', '+')
        #print (opc_fix)
        api_url = 'https://cornershopapp.com/api/v3/branch_groups?locality='+opc_fix+'&country=CL'
        #print(api_url)
        driver.get(api_url)
        time.sleep(1)
        content_api = driver.page_source.encode("utf-8").strip()
        soup_api = BeautifulSoup(content_api, "html.parser")
        
        api_content = soup_api.find('pre').text
        #print(api_content)
        
        data = json.loads(api_content)
        print(opc_fix)   
        
        
        #posiciones en el array (0 = Jumbo; 1 = Sta Isable; 2 = Unimarc; 3 = Jumbo)
        for i in [0,1,2,3]:
            temp = data[0]['items'][i]['content']['store_id']
            
            if temp == '4' or temp == '1' or temp == '611':
                name = data[0]['items'][i]['content']['name']
                id_ = data[0]['items'][i]['content']['id']
                store_id = data[0]['items'][i]['content']['store_id']
                
                store_data = {'store_id':store_id, 'store_name':name, 'id':id_, 'comuna': opc_fix}
                
                result_data.append(store_data)


    else:
        print('saltar')

df = pd.DataFrame(result_data)
                
df.head(0).to_sql(config.CORNERSHOP_STORES['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents_img = output.getvalue()
cur.copy_from(output, config.CORNERSHOP_STORES['table_lz'], null="") # null values become ''
conn.commit()        
connection.close()
driver.quit()
        
        