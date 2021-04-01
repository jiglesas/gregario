# -*- coding: utf-8 -*-
from selenium import webdriver
import time
import log
import pandas as pd
import config
import io
import threading
import json
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 1
#encabezado log
print('Log VM-'+str(id_vm))
print('HORA INICIO: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('Start', 'Lider VM '+str(id_vm))

def insert_db(informacion):
    df = pd.DataFrame(informacion)
    df = df.reindex(columns=['store_id','category_url','product_url','name','sku','brand','price','format','status','create_date', 'product_number', 'normal_price', 'label_price', 'array_position', 'offer'])

    #conexion a la db
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

  
    df.head(0).to_sql(config.LIDER_PRODUCT_LIST['table_lz'], engine, if_exists='append',index=False)
   
                     
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.LIDER_PRODUCT_LIST['table_lz'], null="") # null values become ''
    conn.commit()
    return


def get_product_list(store_id, driver):
    info = []
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

    connection = engine.connect()

    result_query = connection.execute(config.LIDER_PRODUCT_LIST['query_category_prod']).fetchall()
    connection.close()
    
    for wp in result_query:
        
        # se obtienen los paramentros de categorias
        var_temp = json.dumps(wp[0])
        aisle_in = json.loads(var_temp)
        # Se inserta la página web donde se generará el scraper.
        page_link = 'https://www.lider.cl'+aisle_in['category_url']+'?N=&No=0&Nrpp=100000&isNavRequest=Yes'
           
            
        try: 
            driver.get(page_link)
            time.sleep(1)
            content = driver.page_source.encode("utf-8").strip()
            selen = BeautifulSoup(content, "lxml")
           
           
            # Se parsea el texto a html.
            #page_content = BeautifulSoup(page_response.content, "html.parser")
            div_content = selen.find_all("div", {"class": "product-item-box"})
       
            position = 0   
            for div in div_content:
                position = position+1
                store_id = store_id
                brand = div.find( class_ = 'product-name').text   
                name = div.find( class_ = 'product-description').text
                format_ = div.find( class_ = 'product-attribute').text
                sku_ref = div.find(class_ = 'reference-code').text
                product_number = div['prod-number']
                
                price = div.find('b').text
                price = price.replace('Ahorro:', ' ')
                
                try:
                    prices_div = div.find("span", {"class": "price-internet"})
                    price_offer = prices_div.find('b').text
                except:
                    price_offer =  '0'
                    
                try:
                    label_price = div.find("span", {"class": "label-llevamas_fondo"}).text
                except:
                    label_price = '0'
                
                if price_offer != '0' or label_price != '0':
                    offer = True
                else:
                    offer = False
                    
                    
                url_product = div.find('a', {"class": "product-link"})['href']
                #print(url_product)
                div2_content = div.find("div", {"class": "product-addtocart"})
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
       
                status = True
                status_temp = []
                status_temp = div2_content['class']
                for i in range(0, len(status_temp)):
                    if i == 1:
                        status = False                
                #print(status)
                    
                data = {'store_id': store_id, 'category_url' : wp[0]['category_url'], 'product_url': url_product, 'name': name, 'sku': sku_ref,
                        'brand': brand, 'price': price, 'format': format_, 'status': status, 'create_date': date, 'product_number': product_number,
                        'normal_price': price_offer, 'label_price': label_price,'array_position': position, 'offer':offer}
                
                info.append(data)
        except:
            pass
    return info

#def pick_store(store_id, region_position, commune_position, store_position, inicio, fin):
def pick_store(inicio, fin):
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--test-type")
    #options.add_argument('start-maximized')
    options.add_argument('--headless')
    #driver = webdriver.Chrome('./chromedriver.exe', options=options)
    driver = webdriver.Chrome(options=options)
    driver.get('https://www.lider.cl/supermercado/')
   
    local = []
    sublocal = []
    for i in range(inicio,fin):
        #print(result_stores[i])
        local.append(result_stores[i])
        
    #balance de carga en las VM
    num_vm = 8
    num_local = len(local)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = (num_local//num_vm) * 4
    vm5 = (num_local//num_vm) * 5
    vm6 = (num_local//num_vm) * 6
    vm7 = (num_local//num_vm) * 7
    vm8 = num_local
    
    #asignacion de carga segun el id_vm
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(local[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(local[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(local[k])
    elif id_vm == 4:
        for k in range(vm3, vm4):
            sublocal.append(local[k])
    elif id_vm == 5:
        for k in range(vm4, vm6):
            sublocal.append(local[k])
    else:
        for k in range(vm6, vm8):
            sublocal.append(local[k])
            
    print(sublocal)
       
    for ps in sublocal:
        try:
            json_temp = json.dumps(ps[0])
            stores_data = json.loads(json_temp)
            #store_in = stores_in['store_id']
            region_position = stores_data['region_position']
            commune_position = stores_data['commune_position']
            store_position = stores_data['store_position']
            store_name = stores_data['store_name']
            #print(store_id)
           
            #pincha el boton carrito de compra
            driver.find_element_by_class_name('quickcart-new').click()
            time.sleep(2) 
            #selecciona el cambio de ubicacion
            driver.find_element_by_class_name('btn-link').click()
            time.sleep(2)
            #selecciona la opcion de pickup
            driver.find_element_by_class_name('btn-qc-second').click()
            time.sleep(2)
           
            driver.find_element_by_xpath('//*[@id="regions-hasSupermarketPickup"]/div/button/span[1]').click()
            time.sleep(2)
            
            #selecciona una regiones
            driver.find_element_by_xpath('//*[@id="regions-hasSupermarketPickup"]/div/div/ul/li['+str(region_position)+']').click()
            time.sleep(1)
            
            #selecciona una comuna
            driver.find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/button/span[1]').click()
            driver.find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/div/ul/li['+str(commune_position+1)+']').click()
            time.sleep(6)
            
            #seleciona una tienda
            driver.find_element_by_xpath('//*[@id="localeCommunes-hasSupermarketPickup"]/div['+str(store_position+1)+']/label').click()
            time.sleep(2)
            
            driver.find_element_by_xpath('//*[@id="btn-shipping-service-pickup"]').click()
            time.sleep(5)
            
            info_temp = get_product_list(store_name, driver)
            insert_db(info_temp)
        except:
            driver.get('https://www.lider.cl/supermercado/')  
    return


engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result_stores = connection.execute(config.LIDER_STORES['query_stores_prod']).fetchall()
connection.close()
#print(result_stores[0])

#for rs in result_stores:
#    json_temp = json.dumps(rs[0])
#    store_in = json.loads(json_temp)
#    #print(algo2['store_name'])
#    pick_store(store_in['store_name'], store_in['region_position'], store_in['commune_position'], store_in['store_position'])
    
    
#concurrencia
#result_stores = connection.execute(config.LIDER_STORES['query_stores_prod']).fetchall()

#ajuste de hilos y particion del los resultados obtenidos
len_stores = len(result_stores)
#print(len_stores)
n_thread = 2
i0 = 0
i1 = len_stores//n_thread
#i2 = (len_stores//n_thread) * 2
#i3 = (len_stores//n_thread) * 3
i2 = len(result_stores)

#distribucion de hilos
t1 = threading.Thread(name="hilo1", target=pick_store, args=(i0,i1))
t2 = threading.Thread(name="hilo2", target=pick_store, args=(i1,i2))
#t3 = threading.Thread(name="hilo3", target=pick_store, args=(i2,i3))
#t4 = threading.Thread(name="hilo4", target=pick_store, args=(i3,i4))

#ejecuta los hilos
t1.start()
t2.start()
#t3.start()
#t4.start()

#espera a que terminen los hilos, para continuar
t1.join()
t2.join()
#t3.join()
#t4.join()


#data_stores_json = json.dumps(result_stores)
#print(data_stores_json)

#for i in range(0, len(result_stores)):
    #get_product(int(result_stores[i]['store_id']), str(result_stores[i]['category_id']), 0)
    #print(result_stores[i])
    #json_temp = json.dumps(result_stores[i])
    #var_temp = json.loads(json_temp)
    #print(var_temp['store_name'])
    #pick_store(result_stores[i]['store_id'], result_stores[i]['region_position'], result_stores[i]['commune_position'], result_stores[i]['store_position'])


#pie log
print('HORA Fin: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('End', 'Lider VM '+str(id_vm))



    
    


