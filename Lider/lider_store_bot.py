# -*- coding: utf-8 -*-
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import time
import pandas as pd
import config
import io
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event


options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get('https://www.lider.cl/supermercado/')

def get_region_name(nro):
    region = {2: 'ATACAMA', 3: 'LOS RIOS', 4: 'MAGALLLANES Y DE LA ANTARTICA CHILENA', 5: 'ARICA Y PARINACOTA', 6: 'TARAPACA', 7: 'ANTOFAGASTA',
              8: 'COQUIMBO', 9: 'LIBERTADOR GENERAL BERNARDO OHIGGINS', 10: 'MAULE', 11: 'LA ARAUCANIA', 12: 'LOS LAGOS', 13: 'NUBLE',
              14: 'METROPOLITANA DE SANTIAGO', 15: 'VALPARAISO', 16: 'BIO BIO'}
    return region.get(nro, "error de region")

def insert_db(informacion):
    df = pd.DataFrame(informacion)

    #conexion a la db
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

    df.head(0).to_sql(config.LIDER_STORES['table_lz'], engine, if_exists='append',index=False)
                      
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.LIDER_STORES['table_lz'], null="") # null values become ''
    conn.commit()

    return

result = []
store_id = 0
#pincha el boton carrito de compra
button_temp = driver.find_element_by_class_name('quickcart-new').click()
time.sleep(2) 
#selecciona el cambio de ubicacion
change_temp = driver.find_element_by_class_name('btn-link').click()
time.sleep(2)
#selecciona la opcion de pickup
pickup_temp = driver.find_element_by_class_name('btn-qc-second').click()
time.sleep(2)


ul_temp = driver.find_element_by_xpath('//*[@id="regions-hasDelivery"]/div/div/ul')

all_li = ul_temp.find_elements_by_tag_name("li")

time.sleep(2)
btn_list = driver.find_element_by_xpath('//*[@id="regions-hasSupermarketPickup"]/div').click

content = driver.page_source.encode("utf-8").strip()
selen = BeautifulSoup(content, "lxml")
div_content = selen.find_all("div", {"class": "product-item-box"})

#se recorre la lista de regiones
i = 15
while i < len(all_li):
    #pinchamos el boton de regiones
    list_temp = driver.find_element_by_xpath('//*[@id="regions-hasSupermarketPickup"]/div/button/span[1]').click()
    time.sleep(2)
    
    if i > 1:
        #selecciona un elemento de la lista de regiones(se salta el SELECCIONAR REGION:)
        list_temp_otro = driver.find_element_by_xpath('//*[@id="regions-hasSupermarketPickup"]/div/div/ul/li['+str(i)+']').click()
        time.sleep(2)
    
        #se pincha la lista de comunas
        commune_list_temp = driver.find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/button/span[1]').click()
        time.sleep(2)
        
        #se carga la lista de comunas
        commune_temp = all_li[i].find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/div/ul')
        all_commune = commune_temp.find_elements_by_tag_name("li")
        #print(len(all_commune))
        time.sleep(3)
        
        #se captura la informacion de la lista regiones
        '''
        text = all_li[i].get_attribute("innerHTML")
        print(text)
        selen = BeautifulSoup(text, "lxml")
        div_content = selen.find("span", {"class": "text"})
        region_name = div_content.text
        print(region_name)
        '''
        
        #recorre la lista de comunas
        j = 0
        while j < len(all_commune):
            if j>0:
                #print(str(j)+'region')
                driver.find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/div/ul/li['+str(j+1)+']').click()
                time.sleep(3)
                driver.find_element_by_xpath('//*[@id="communes-hasSupermarketPickup"]/div/button/span[1]').click()
            
                store_temp = all_commune[j].find_element_by_xpath('//*[@id="localeCommunes-hasSupermarketPickup"]')
                all_stores = store_temp.find_elements_by_tag_name("div")
                
                text_temp = all_commune[j].get_attribute("innerHTML")
                selen = BeautifulSoup(text_temp, "lxml")
                div_content_2 = selen.find("span", {"class": "text"})
                if j>0:
                    commune_name = div_content_2.text
                    print(commune_name)
                
                #recorre las tiendas
                k= 0
                if commune_name != 'LO PRADO':
                    while k < len(all_stores)-1:
                        store_id = store_id + 1
                        st_tp = all_stores[k].get_attribute("innerHTML")
                        selen2 = BeautifulSoup(st_tp, "lxml")
                        #print(selen2)
                        
                        div_content_3 = selen2.find("span", {"class": "text fw-bold"})
                        try:
                            store_id_lider = selen2.find("input", {"class": "cd-radio"})['store-id']
                        except:
                            store_id_lider = 9999999999
                        print(store_id_lider)
                        store_name = div_content_3.text
                        print(store_name)
                        region_name = get_region_name(i)
                        
                        data = {"store_id": str(store_id_lider) ,"region": region_name, "commune": commune_name, "store_name": store_name,
                                "region_position": i, "commune_position": j, "store_position": k}
                        result.append(data)
                        k += 1
                        time.sleep(2)
                        insert_db(result)
                        result=[]

            j += 1
            time.sleep(2)
    
    i += 1


