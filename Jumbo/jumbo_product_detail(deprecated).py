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
from PIL import Image
import pyrebase

#inicializacion del webdriver
url_root = 'https://www.jumbo.cl'
options = webdriver.ChromeOptions()
#options.add_argument('--ignore-certificate-errors')
#options.add_argument('--incognito')
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
#driver.get(url_root)
#time.sleep(5)


result = []

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
result = connection.execute(config.JUMBO_PRODUCT_DETAIL['query_product']).fetchall()
firebase = pyrebase.initialize_app(config.FIREBASE_CONFIG)
db = firebase.database()

for wp in result:
    path_pro = url_root + wp[0]
    driver.get(path_pro)
    
    time.sleep(4)
    print(path_pro)
    #comprobacion si el articulo aun existe
    if path_pro == driver.current_url:
        content = driver.page_source.encode("utf-8").strip()
        soup = BeautifulSoup(content, "lxml")
        
        #div_content = soup.find_all("span", {"class": "product-item-box"})
        if soup.find( class_ = 'product-code').text:
            sku_num = soup.find( class_ = 'product-code').text
        else:
            sku_num_num = 'error'
        
        sku_temp = sku_num.split(" ")
        sku = sku_temp[1]
            
        
        name = soup.find( class_ = 'product-name').text
        brand = soup.find( class_ = 'product-brand').text
        format_ = soup.find( class_ = 'product-single-unit').text
        status = True        
        breadcrumb = soup.find("ul").text
        offer = False
        if soup.find("span", {"class": "product-flag-text"}):
            offer = True
        #print(offer)
        
        #captura de diferentes valores precio
        if soup.find("span",{"class": "price-product-title-ppum"}):
            value_det1 = soup.find("span",{"class": "price-product-title-ppum"}).text
        else:
            if soup.find("span", {"class": "product-price-ppum"}):
                value_det1 = soup.find("span", {"class": "product-price-ppum"}).text
            else:
                value_det1 = 'no aplica'
        
        
        #logica para capturar el precio MEJORAR
        precio_normal = ''
        if soup.find( class_ = 'product-single-price-container'):
            precio_normal = soup.find( class_ = 'product-single-price-container').text
        #print(precio_normal)
        
        mecanica_promo = ''
        if soup.find("span", {"class": "price-product-prom"}):
            mecanica_promo = soup.find("span", {"class": "price-product-prom"}).text
        #print (mecanica_promo)
        
        precio_promo = []
        precio_promo1 = ''
        precio_promo2 = ''
        if soup.find( class_ = 'price-product-best'):
            for precio_temp in soup.findAll("span", {"class": "price-product-best"}):
                precio_promo.append(precio_temp.text)
            
            precio_promo1 = precio_promo[0]
            #
            for i in range(0, len(precio_promo)):
                if i == 1:
                    precio_promo2 = precio_promo[1]
        
        
        precio_sin_promo = ''
        if soup.find("span", {"class": "price-product-value"}):
            precio_sin_promo = soup.find("span", {"class": "price-product-value"}).text
        #print(precio_sin_promo)
        
        '''
        if soup.find("div",{"class": "product-single-price-container"}):
            x= soup.find("div",{"class": "product-single-price-container"})
            print(x.text)
            
        if soup.find( class_ = 'product-single-price-container'):
            price = soup.find( class_ = 'product-single-price-container').text
        else:
            if soup.find( class_ = 'price-product-best'):
                price = soup.find( class_ = 'price-product-best').text
                '''
        #print(price)
        rut = '93834000-5'
        img_thumb = ''
        #logica para la captura de imagenes (solo esta captando todas las urls, no almacenando)
        if soup.find("div", {"class": "product-image-thumbs"}):
            temp_imgs = soup.findAll("div", {"class": "product-image-thumbs"})
            img_urls= []
            for temp_img in temp_imgs:
                urls = temp_img.findAll('img')
                
                for url in urls:
                    img_urls.append(url['src'])
            #imprime solo la primera foto del arreglo    
            #print(img_urls[0])
            for i in range(0, len(img_urls)):
                #print(img_urls[i])
                replace_url = img_urls[i].replace("100-100", "1000-1000")
                png_url = replace_url.replace(".jpg", ".png")
                r = requests.get(png_url, stream = True)
        
                img = Image.open(io.BytesIO(r.content))
                
                if img.mode != 'RGB':
                    #img = img.convert('RGB')
                    img_path = rut+'/'+sku+'-'+str(i)+'.png'
                    #print(img_path)
                    img.save(img_path)
                    
                    firebase_path = "scrapers/"+rut+"/"+sku+"/"+sku+'-'+str(i)+".png"
                    storage = firebase.storage()
                    storage.child(firebase_path).upload(img_path)
                    
                    img_data = {'sku': [sku], 'path': [firebase_path], 'url': [img_urls[i]]}
                    df_img = pd.DataFrame(img_data)
            
                    df_img.head(0).to_sql(config.JUMBO_PRODUCT_DETAIL['table_img'], engine, if_exists='append',index=False)
                
                    conn_img = engine.raw_connection()
                    cur_img = conn_img.cursor()
                    output_img = io.StringIO()
                    df_img.to_csv(output_img, sep='\t', header=False, index=False)
                    output_img.seek(0)
                    contents_img = output_img.getvalue()
                    cur_img.copy_from(output_img, config.JUMBO_PRODUCT_DETAIL['table_img'], null="") # null values become ''
                    conn_img.commit()
            
        else:
            if soup.find("div", {"class": "zoomed-image"}):
                img = soup.find("div", {"class": "zoomed-image"})['style']
                img_url = img.split('"')       
                #se hace un split para tomar solo la url
                imagen_temp = img_url[1]
                imagen_final = imagen_temp.replace("750-750", "1000-1000")

                r = requests.get(imagen_final, stream = True)
        
                img = Image.open(io.BytesIO(r.content))
                if img.mode != 'RGB':
                    #img = img.convert('RGB')
                    img_path = rut+'/'+sku+'.png'
                    #print(img_path)
                    img.save(img_path)
                    
                    firebase_path = "scrapers/"+rut+"/"+sku+"/"+sku+".jpg"
                    storage = firebase.storage()
                    storage.child(firebase_path).upload(img_path)
                    
                    img_data = {'sku': [sku], 'path': [firebase_path], 'url': [imagen_final]}
                    df_img = pd.DataFrame(img_data)
            
                    df_img.head(0).to_sql(config.JUMBO_PRODUCT_DETAIL['table_img'], engine, if_exists='append',index=False)
                
                    conn_img = engine.raw_connection()
                    cur_img = conn_img.cursor()
                    output_img = io.StringIO()
                    df_img.to_csv(output_img, sep='\t', header=False, index=False)
                    output_img.seek(0)
                    contents_img = output_img.getvalue()
                    cur_img.copy_from(output_img, config.JUMBO_PRODUCT_DETAIL['table_img'], null="") # null values become ''
                    conn_img.commit()
            
        
        data = {'name': [name], 'brand':[brand], 'sku_num': [sku],   'status': [status], 'format' : [format_], 'breadcrumb': [breadcrumb], 'value_det1': [value_det1], 'offer': [offer],
                'mecanica_promocion': [mecanica_promo], 'normal_price': [precio_normal], 'promotion_price_1': [precio_promo1], 'promotion_price_2': [precio_promo2], 'no_promotion_price': [precio_sin_promo], 'product_url': [wp[0]]}
        
        result.append(data)

        
    #si no existe el producto duerme por 3 seg    
    else:
        time.sleep(3)
        
        df = pd.DataFrame(result)
        #print(df)
        
#subir dataframe a la bd
df.head(0).to_sql(config.JUMBO_PRODUCT_DETAIL['table_lz'], engine, if_exists='append',index=False)
        
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, config.JUMBO_PRODUCT_DETAIL['table_lz'], null="") # null values become ''
conn.commit()
    
gc.collect()


connection.close()
    