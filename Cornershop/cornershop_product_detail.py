from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import time
import json
import log
import requests 
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
from datetime import datetime
import io
from random import randrange

#api sample
#https://cornershopapp.com/api/v1/branches/12630/aisles/C_15/products
result_aisles = []
result_stores = []
result = []

#inicializacion del webdriver
'''
options = webdriver.ChromeOptions()
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
'''

# Se entrega el agente de browser o entrega error de acceso.
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

#obtenemos las tiendas y pasillos a scrapear
result_stores = connection.execute(config.CORNERSHOP_STORES['query_stores']).fetchall()
result_aisles = connection.execute(config.CORNERSHOP_AISLES['query_aisles']).fetchall()

for wp_s in result_stores:
    for wp_a in result_aisles:
        print('https://cornershopapp.com/api/v1/branches/'+wp_s[0]+'/aisles/'+wp_a[0]+'/products')
        api_url = 'https://cornershopapp.com/api/v1/branches/'+wp_s[0]+'/aisles/'+wp_a[0]+'/products'
        
        #driver.get(api_url)
        page_response = requests.get(api_url, headers=headers, timeout=30)
        # Se parsea el texto a html.
        page_content = BeautifulSoup(page_response.content, "html.parser")
        
        
        #content_api = driver.page_source.encode("utf-8").strip()
        #soup_api = BeautifulSoup(content_api, "html.parser")
        #api_content = soup_api.find('pre').text
        #data = json.loads(api_content)
        print(page_content)
        data = json.loads(str(page_content))
        
        items = len(data)
        for item in range(0,items):
            aisle_id = wp_a[0]
            store_id = wp_s[0]
            name = data[item]['name']
            id_ = data[item]['id']
            offer_price = data[item]['price']
            price_wo_offer = data[item]['original_price']
            price_per_unit = data[item]['price_per_unit']
            package  = data[item]['package']
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = data[item]['availability_status']
            #marca
            if data[item]['brand'] != None:
                brand = data[item]['brand']['name']
            else:
                brand = 'no registrado'
            #oferta
            if price_wo_offer != None:
                offer = True
            else:
                offer = False
            
            #print(aisle_id)
            info = {'store_id': store_id, 'aisle_id': aisle_id, 'name': name, 'id':id_, 'offer_price': offer_price,
                    'price_wo_offer': price_wo_offer, 'price_per_unit': price_per_unit, 'package': package,
                    'status': status, 'brand': brand, 'offer': offer, 'create_date': date}
            
            result.append(info)
            
        time.sleep(randrange(2))
    
df = pd.DataFrame(result)
    
df.head(0).to_sql(config.CORNERSHOP_PRODUCT_DETAIL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.CORNERSHOP_PRODUCT_DETAIL['table_lz'], null="") # null values become ''
conn.commit()
connection.close()
#driver.close()