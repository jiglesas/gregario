# -*- coding: utf-8 -*-
import time
from bs4 import BeautifulSoup
import pandas as pd
import config
import log
from datetime import datetime
import io
import requests 
import json
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 6
api_key = 'IuimuMneIKJd3tapno2Ag1c1WcAES97j'
result = []

#obtiene las tiendas desde la db
def get_stores():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_stores = connection.execute(config.JUMBO_STORES['query_stores_prod']).fetchall()
    connection.close()
    return result_stores

#obtiene las categorias desde la db
def get_category():
    categories = []
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_category = connection.execute(config.JUMBO_CATEGORY['query_category_prod']).fetchall()
    for rs in result_category:
        json_temp = json.dumps(rs[0])
        stores_in = json.loads(json_temp)
        categories.append(stores_in['category_url'])
    connection.close()
    return categories

def balance_cargas():
    sublocal = []     
    #balance de carga en las VM, se distribuyen la cantidad de locales entrantes 
    num_vm = 6
    result_stores = get_stores()
    num_local = len(result_stores)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = (num_local//num_vm) * 4
    vm5 = (num_local//num_vm) * 5
    vm6 = num_local
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(result_stores[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(result_stores[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(result_stores[k])
    elif id_vm == 4:
        for k in range(vm3, vm4):
            sublocal.append(result_stores[k])
    elif id_vm == 5:
        for k in range(vm4, vm5):
            sublocal.append(result_stores[k])
    else:
        for k in range(vm5, vm6):
            sublocal.append(result_stores[k])
              
    return sublocal


def get_url(store_id, category, pag):
    url = 'https://apijumboweb.smdigital.cl/catalog/api/v2/products/search'+category+'?page='+str(pag)+'&sc='+str(store_id)
    #print(url)
    return url

def insert_db(data_in):                       
    df = pd.DataFrame(data_in)    
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    
    df = df.reindex(columns=['store_id','category','product_id','product_name','product_code','brand','product_url','status','formato','price','price_wo_offer', 'create_date', 'offer'])
    df.head(0).to_sql(config.JUMBO_PRODUCT_LIST['table_lz'], engine, if_exists='append',index=False)
                    
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.JUMBO_PRODUCT_LIST['table_lz'], null="") # null values become ''
    conn.commit()
    return

#encabezado log
print('Log VM-'+str(id_vm)+' Jumbo')
print('HORA INICIO: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('Start', 'Jumbo VM '+str(id_vm))

store = balance_cargas()
category = get_category()
print(store)

#for rs in store:
for rs in range(0,1):
    for rc in category:
        #ciclo de paginas             
        i = 0
        flag = True
        while flag == True:
            i += 1
            
            url = get_url('25',rc,i)
            hed = {'x-api-key': api_key}
            y = requests.get(url, headers=hed)

            try:
                if y.status_code == 200:
                    data = json.loads(y.text)
                    
                    if data == []:
                        flag = False
                    else:
                        #capturamos los datos de la api
                        for j in range(1,len(data)):
                            try:
                                product_code = data[j]['productId']
                            except:
                                product_code = 'error'
                            product_name = data[j]['productName']
                            product_id = data[j]['productReference']
                            brand = data[j]['brand']
                            product_url = data[j]['linkText']
                            status = 'Disponible'
                            create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            try:
                                formato = str(data[j]['items'][0]['unitMultiplier'])+' '+str(data[j]['items'][0]['measurementUnit'])
                            except:
                                formato = 'error'
                                
                            #precios
                            price = data[j]['items'][0]['sellers'][0]['commertialOffer']['Price']
                            price_wo_offer = data[j]['items'][0]['sellers'][0]['commertialOffer']['PriceWithoutDiscount']
                            if int(price)-int(price_wo_offer) == 0:
                                offer = True
                            else:
                                offer = False
                            
                            #estado
                            avalible = int(data[j]['items'][0]['sellers'][0]['commertialOffer']['AvailableQuantity'])
                            #print(avalible)
                            if avalible < 5:
                                if avalible == 0:
                                    status = 'Agotado'
                                    #print(product_name)
                                else:
                                    status = 'Posible Quiebre'
                                    #print(status)
                            
                            info = {'store_id': '25', 'category': rc, 'product_id': product_id, 'product_name': product_name,
                                    'product_code': product_code, 'brand': brand, 'product_url': product_url, 'status': status,
                                    'formato': formato, 'price': price, 'price_wo_offer': price_wo_offer, 'create_date': create_date,
                                    'offer': offer}
                            
                            result.append(info)
            except:
                pass
    
insert_db(result)
print('HORA FIN: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
log.insert_log('End', 'Jumbo VM '+str(id_vm))
     

            
            
            





