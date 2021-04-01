from bs4 import BeautifulSoup
import requests 
import pandas as pd
# pip install psycopg2
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io
import time
import pyrebase
from PIL import Image

import config

result = []

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

result = connection.execute(config.LIDER_PRODUCT_DETAIL['query_product']).fetchall()

#rut retail(lider)
rut = '76134941-4'

#inicializa firebase
firebase = pyrebase.initialize_app(config.FIREBASE_CONFIG)
db = firebase.database()

for wp in result:
    
    # Se inserta la página web donde se generará el scraper.
    page_link = 'https://www.lider.cl'+wp[0]
    
    #se obtiene el codigo de producto de la pagina lider (no SKU)
    product_id = wp[0].split("/")[4]
    
    #url de imagen (definir para a-b-c-etc...)
    #img_url = 'https://images.lider.cl/wmtcl?source=url[file:/productos/'+product_id+'a.jpg&sink'
        
    
    # Se entrega el agente de browser o entrega error de acceso.
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    # # Se hace un request a la página y queda todo almacenado en la variable.
    page_response = requests.get(page_link, headers=headers, timeout=30)
    
    # Se parsea el texto a html.
    page_content = BeautifulSoup(page_response.content, "html.parser")
    #we use the html parser to parse the url content and store it in a variable.
    
    #capturar atributos principales de producto
    sku_num = page_content.find( id = 'item-number-id').text
    brand = page_content.find( itemprop = 'brand').text
    name = page_content.find( id = 'span-display-name').text
    price = page_content.find( itemprop = 'lowPrice').text
    breadcrumb = page_content.find( class_ = 'breadcrumb col-md-12 hidden-xs').text
    if page_content.find(class_="btn btn-info btn-block btn-agregar js-btn-agregar-pdp"):     
        status= True
    else:
        status= False
    
    formatos = []
    formats_ = page_content.find('h1')
    for format_ in formats_:
        formatos.append(format_)
    
    formato = formatos[3].text
    
    precios1 = []
    precios = page_content.find(id = "productPrice")
    for precio in precios:
        precios1.append(precio)
    
    value_det1 = precios1[2].text
    value_det2 = precios1[3].text
    
    #carga de imagenes existentes del producto a firebase
    '''
    photo_array = ['a', 'b', 'c', 'd', 'e']
    for x in photo_array:
        img_url = 'https://images.lider.cl/wmtcl?source=url[file:/productos/'+product_id+x+'.jpg&sink'
        #print(img_url)
        r = requests.get(img_url, stream = True)
        
        img = Image.open(io.BytesIO(r.content))
        if img.mode != 'P':
            #img = img.convert('RGB')
            img_path = rut+'/'+sku_num+x+'.jpg'
            img.save(img_path , 'JPEG')
            
            firebase_path = "scrapers/"+rut+"/"+sku_num+"/"+sku_num+x+".jpg"
            storage = firebase.storage()
            storage.child(firebase_path).upload(img_path)
            
            img_data = {'sku': [sku_num], 'path': [firebase_path]}
            df_img = pd.DataFrame(img_data)
            
            df_img.head(0).to_sql(config.LIDER_PRODUCT_DETAIL['table_img'], engine, if_exists='append',index=False)
                
            conn_img = engine.raw_connection()
            cur_img = conn_img.cursor()
            output_img = io.StringIO()
            df_img.to_csv(output_img, sep='\t', header=False, index=False)
            output_img.seek(0)
            contents_img = output_img.getvalue()
            cur_img.copy_from(output_img, config.LIDER_PRODUCT_DETAIL['table_img'], null="") # null values become ''
            conn_img.commit()
        '''    

    
    data = {'name': [name], 'brand':[brand], 'sku_num': [sku_num], 'price': [price], 'product_id': [product_id], 'breadcrumb': [breadcrumb], 'status': [status], 'format' : [formato], 'value_det1': [value_det1], 'value_det2': [value_det2]}
    df = pd.DataFrame(data)
    #print(df)
    
    #subir dataframe a la bd
    df.head(0).to_sql(config.LIDER_PRODUCT_DETAIL['table_lz'], engine, if_exists='append',index=False)
    
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, config.LIDER_PRODUCT_DETAIL['table_lz'], null="") # null values become ''
    conn.commit()
    
gc.collect()


connection.close()
