from bs4 import BeautifulSoup
import requests 
import pandas as pd
# pip install psycopg2
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io
import time
from selenium import webdriver
import config
from datetime import datetime

#inicializacion del webdriver
url_root = 'https://www.jumbo.cl'
options = webdriver.ChromeOptions()
#options.add_argument('--ignore-certificate-errors')
#options.add_argument('--incognito')
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get(url_root)
time.sleep(15)

info = []
result = []

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
result = connection.execute(config.JUMBO_PRODUCT_LIST_ALL['query_category']).fetchall()

for wp in result:
    print(wp)
    driver.get(url_root + wp[0])
    time.sleep(4)
    content = driver.page_source.encode("utf-8").strip()
    soup = BeautifulSoup(content, "lxml")
    
    #iteracion por cada pagina de la seccion
    cant_pag = soup.findAll("button", class_= "page-number")
    for pag in cant_pag:
        print(pag.text)
        path_pro = url_root + wp[0]+'?page='+pag.text
        driver.get(path_pro)
        time.sleep(3)
        if path_pro == driver.current_url:
            content = driver.page_source.encode("utf-8").strip()
            soup = BeautifulSoup(content, "lxml")
            
            #captura de datos de los contedores
            for div in soup.findAll("li", {"class": "shelf-item"}):
                name = div.find("a", {"class": "shelf-product-title"}).text
                url = div.find("a" , {"class": "shelf-wrap-image"} )['href']
                brand = div.find("h3", {"class": "shelf-product-brand"}).text
                format_ = div.find("span", {"class": "shelf-single-unit"}).text
                date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
                if div.find("span", {"class": "product-add-cart-text"}):
                    stock = True
                else:
                    stock = False
                    
                if div.find("div", {"class": "product-single-price-container"}):
                    price = div.find("div", {"class": "product-single-price-container"}).text
                    offer = False
                elif div.find("span", {"class": "price-product-best"}):
                    price = div.find("span", {"class": "price-product-best"}).text
                    offer = True
                else:
                    price = 'error'
                    offer = False
                
                product_list_jumbo = {'store_id': '16','category_url': wp[0], 'product_url': url, 'name': name, 'brand': brand,
                                      'formato': format_, 'price': price, 'creation_date': date, 'stock': stock, 'offer': offer}
           
                info.append(product_list_jumbo)     
        
        else:
            time.sleep(1)

df_cat = pd.DataFrame(info)
df_cat.head(0).to_sql(config.JUMBO_PRODUCT_LIST_ALL['table_lz_selected'], engine, if_exists='append',index=False)
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df_cat.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, config.JUMBO_PRODUCT_LIST_ALL['table_lz_selected'], null="") # null values become ''
conn.commit()    
connection.close
driver.close()