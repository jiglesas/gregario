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

result = []
info = []

options = webdriver.ChromeOptions()
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
#driver.get(url_root)

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result = connection.execute(config.LIDER_PRODUCT_LIST['query_category']).fetchall()

for wp in result:
    
    # Se inserta la página web donde se generará el scraper.
    page_link = 'https://www.lider.cl'+wp[0]+'?N=&No=0&Nrpp=100000&isNavRequest=Yes'
    
    #print(page_link)
    
    driver.get(page_link)
    time.sleep(2)
    content = driver.page_source.encode("utf-8").strip()
    selen = BeautifulSoup(content, "lxml")

    # Se entrega el agente de browser o entrega error de acceso.
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    # # Se hace un request a la página y queda todo almacenado en la variable.
    page_response = requests.get(page_link, headers=headers, timeout=30)
    
    
    # Se parsea el texto a html.
    #page_content = BeautifulSoup(page_response.content, "html.parser")
    div_content = selen.find_all("div", {"class": "product-item-box"})

    #product_list_array = []
    #product_list_brand = []
        
    for div in div_content:

        brand = div.find( class_ = 'product-name').text   
        name = div.find( class_ = 'product-description').text
        format_ = div.find( class_ = 'product-attribute').text
        sku_ref = div.find(class_ = 'reference-code').text
        price = div.find('b').text 
        print(price)
        price = price.replace('Ahorro:', ' ')
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
                
        
        a_content = div.find_all('div', {"class": "box-product"} )
                
        #for a in a_content:
            #print(a['href'])
            #product_list_array.append(a['href'])
            
        data = { 'category_url' : wp[0], 'product_url': url_product, 'name': name, 'sku': sku_ref,
                'brand': brand, 'price': price, 'format': format_, 'status': status, 'create_date': date }
        #product_list_lider = {'category_url': [wp[0]], 'product_url': [product['href']]}
        
        info.append(data)
        
df = pd.DataFrame(info)
#subir dataframe a la bd
df.head(0).to_sql(config.LIDER_PRODUCT_LIST_ALL['table_lz'], engine, if_exists='append',index=False)
        
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, config.LIDER_PRODUCT_LIST_ALL['table_lz'], null="") # null values become ''
conn.commit()

time.sleep(2)          
gc.collect()

connection.close()



