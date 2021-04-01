import logging
import config
import json
import pandas as pd
from selenium import webdriver
import io
from bs4 import BeautifulSoup
import requests 
import threading
import time
from random import randrange
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

result=[]
options = webdriver.ChromeOptions()
options.add_argument('start-maximized')

def stores_aisles(inicio,fin):
    #se determina en la variable local, que locales va a scapear
    driver = webdriver.Chrome('./chromedriver.exe', options=options)
    local = []
    for i in range(inicio,fin):
        print(result_stores[i])
        local.append(result_stores[i])
        
    for wp_s in local:
        print(wp_s)
        time.sleep(1)
        for wp_a in result_aisles:
            
            print('https://cornershopapp.com/api/v1/branches/'+str(wp_s[0])+'/aisles/'+str(wp_a[0])+'/products')
            api_url = 'https://cornershopapp.com/api/v1/branches/'+str(wp_s[0])+'/aisles/'+str(wp_a[0])+'/products'
            
            driver.get(api_url)
    
            content_api = driver.page_source.encode("utf-8").strip()
            soup_api = BeautifulSoup(content_api, "html.parser")
            api_content = soup_api.find('pre').text
            data = json.loads(api_content)
            #data = json.loads(str(page_content))
            
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
                
            time.sleep(randrange(2,3))
    driver.close()
    return

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema2'])})

connection = engine.connect()

user_agent_list = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
]


#obtenemos las tiendas y pasillos a scrapear
result_stores = connection.execute(config.CORNERSHOP_STORES['query_stores_selected_2']).fetchall()
result_aisles = connection.execute(config.CORNERSHOP_AISLES['query_aisles']).fetchall()

#ajuste de hilos y particion del los resultados obtenidos
len_stores = len(result_stores)
n_thread = 1
i0 = 0
#i1 = len_stores//n_thread
i1 = len(result_stores)

tiempo_ini = datetime.now()

t1 = threading.Thread(name="hilo1", target=stores_aisles, args=(i0, i1))
#t2 = threading.Thread(name="hilo2", target=stores_aisles, args=(i1, i2))



#ejecuta los hilos
t1.start()
#t2.start()



#espera a que terminen los hilos, para continuar
t1.join()
#t2.join()



df = pd.DataFrame(result)
    
df.head(0).to_sql(config.CORNERSHOP_PRODUCT_SPOT['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.CORNERSHOP_PRODUCT_SPOT['table_lz'], null="") # null values become ''
conn.commit()


tiempo_fin = datetime.now()
print(tiempo_fin.second - tiempo_ini.second)

connection.close()