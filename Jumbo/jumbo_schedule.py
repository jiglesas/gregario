import schedule 
from bs4 import BeautifulSoup
import requests 
import pandas as pd
from selenium import webdriver
import time
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io
import config


#inicializacion del webdriver
url_root = 'https://www.jumbo.cl'
options = webdriver.ChromeOptions()
#options.add_argument('--ignore-certificate-errors')
#options.add_argument('--incognito')
options.add_argument('start-maximized')
#options.add_argument('--headless')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get(url_root)
time.sleep(5)

def jumboCategory():

    
    
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    
    connection = engine.connect()
    
    
    content = driver.page_source.encode("utf-8").strip()
    prueba = BeautifulSoup(content, "lxml")
    
    categorias = prueba.findAll("a", class_="new-header-supermarket-title" )
    
    
    
    for categoria in categorias:
        #funcion de captura para seccion supermercado
        if(categoria['href']) == '/':
            #crear funcion
            time.sleep(5)
            products = prueba.findAll("a" , class_="new-header-supermarket-dropdown-item-name" )
            for product in products:
                print(product['href'])
                category_jumbo = {'category_url': [product['href']]}
                df_cat = pd.DataFrame(category_jumbo)
                
                df_cat.head(0).to_sql(config.JUMBO_CATEGORY['table_lz'], engine, if_exists='append',index=False)
                    
                conn_img = engine.raw_connection()
                cur_img = conn_img.cursor()
                output_img = io.StringIO()
                df_cat.to_csv(output_img, sep='\t', header=False, index=False)
                output_img.seek(0)
                contents_img = output_img.getvalue()
                cur_img.copy_from(output_img, config.JUMBO_CATEGORY['table_lz'], null="") # null values become ''
                conn_img.commit()
                
                
        #captura las demas categorias
        else:
            print(categoria['href'])
            driver.get(url_root + categoria['href'])
            time.sleep(5)
            
            category_jumbo = {'category_url': [categoria['href']]}
            df_cat = pd.DataFrame(category_jumbo)
                
            df_cat.head(0).to_sql(config.JUMBO_CATEGORY['table_lz'], engine, if_exists='append',index=False)
                    
            conn_img = engine.raw_connection()
            cur_img = conn_img.cursor()
            output_img = io.StringIO()
            df_cat.to_csv(output_img, sep='\t', header=False, index=False)
            output_img.seek(0)
            contents_img = output_img.getvalue()
            cur_img.copy_from(output_img, config.JUMBO_CATEGORY['table_lz'], null="") # null values become ''
            conn_img.commit()
    
            content = driver.page_source.encode("utf-8").strip()
            prueba = BeautifulSoup(content, "lxml")
    
    connection.close()
    driver.close()
    
schedule.every(5).seconds.do(jumboCategory)

while True:
    schedule.run_pending()
    time.sleep(2)