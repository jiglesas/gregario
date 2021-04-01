import config
import json
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
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
    driver = webdriver.Chrome(ChromeDriverManager().install())
    #driver = webdriver.Chrome('./chromedriver.exe', options=options)
    local = []
    sublocal = []
    for i in range(inicio,fin):
        #print(result_stores[i])
        local.append(result_stores[i])
        
    #balance de carga en las VM
    id_vm = 3
    num_vm = 4
    num_local = len(local)
    local_temp = num_local//num_vm
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = num_local
    
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(local[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(local[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(local[k])
    else:
        for k in range(vm3, vm4):
            sublocal.append(local[k])
            
    print(sublocal)
        
    for wp_s in local:
        #print(wp_s)
        time.sleep(randrange(1,3))
        #set incoming json
        var_temp = json.dumps(wp_s[0])
        store_in = json.loads(var_temp)
        for wp_a in result_aisles:
            #set incoming json
            json_temp = json.dumps(wp_a[0])
            aisle_in = json.loads(json_temp)
            
            #print('https://cornershopapp.com/api/v1/branches/'+str(store_in['store_id'])+'/aisles/'+aisle_in['aisle_id']+'/products')
            #api_url = 'https://cornershopapp.com/api/v1/branches/'+wp_s[0]+'/aisles/'+wp_a[0]+'/products'
            api_url = 'https://cornershopapp.com/api/v1/branches/'+str(store_in['store_id'])+'/aisles/'+aisle_in['aisle_id']+'/products'
            
            driver.get(api_url)
    
            content_api = driver.page_source.encode("utf-8").strip()
            soup_api = BeautifulSoup(content_api, "html.parser")
            api_content = soup_api.find('pre').text
            data = json.loads(api_content)
            #data = json.loads(str(page_content))
            
            items = len(data)
            for item in range(0,items):
                aisle_id = wp_a[0]
                print(aisle_id['aisle_id'])
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
                info = {'store_id': store_id['store_id'], 'aisle_id': aisle_id['aisle_id'], 'name': name, 'id':id_, 'offer_price': offer_price,
                        'price_wo_offer': price_wo_offer, 'price_per_unit': price_per_unit, 'package': package,
                        'status': status, 'brand': brand, 'offer': offer, 'create_date': date}
                
                result.append(info)
                
            time.sleep(randrange(2,4))
    driver.close()
    return

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

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

proxy_list = [
    '170.0.203.9:1080',
    '84.22.59.202:5678',
    '104.248.48.233:30588',
    '134.249.198.7:7777',
    '203.128.72.62:4145',
    '150.107.103.64:4145',
    '69.163.164.116:18309',
    '103.20.191.242:4145',
    ]

#obtenemos las tiendas y pasillos a scrapear
result_stores = connection.execute(config.CORNERSHOP_STORES['query_stores_prod']).fetchall()
result_aisles = connection.execute(config.CORNERSHOP_AISLES['query_aisles_prod']).fetchall()

#ajuste de hilos y particion del los resultados obtenidos
len_stores = len(result_stores)
n_thread = 4
i0 = 0
i1 = len_stores//n_thread
i2 = (len_stores//n_thread) * 2
i3 = (len_stores//n_thread) * 3
i4 = len(result_stores)

tiempo_ini = datetime.now()

t1 = threading.Thread(name="hilo1", target=stores_aisles, args=(i0, i1))
t2 = threading.Thread(name="hilo2", target=stores_aisles, args=(i1, i2))
t3 = threading.Thread(name="hilo3", target=stores_aisles, args=(i2, i3))
t4 = threading.Thread(name="hilo4", target=stores_aisles, args=(i3, i4))



#ejecuta los hilos
t1.start()
t2.start()
t3.start()
t4.start()



#espera a que terminen los hilos, para continuar
t1.join()
t2.join()
t3.join()
t4.join()



df = pd.DataFrame(result)
    
df.head(0).to_sql(config.CORNERSHOP_PRODUCT_DETAIL_TEMP['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.CORNERSHOP_PRODUCT_DETAIL_TEMP['table_lz'], null="") # null values become ''
conn.commit()


tiempo_fin = datetime.now()
print(tiempo_fin.second - tiempo_ini.second)

connection.close()