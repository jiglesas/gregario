# -*- coding: utf-8 -*-
import requests
import config
from bs4 import BeautifulSoup
import io
import json
import time
from random import randrange
from datetime import datetime
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 2
#obtiene las tiendas desde la db
def get_product(store_id):
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    query = """
            select 
            	a.product_number 
            from
            	history.lider_product_list a
            where 
            	a.load_date::date >= now() + interval '- 10 days'
            	and a.store_id = '"""+store_id+"""'
            group by 
            	1
            """
    

            
    result_product = connection.execute(query).fetchall()
    connection.close()
    return result_product

def get_stores():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_store = connection.execute(config.LIDER_PRODUCT_NUMBER['query_store']).fetchall()
    connection.close()
    return result_store

def insert_db(data_in):                       
    df = pd.DataFrame(data_in)    
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    
    df = df.reindex(columns=['insert_date','product_id', 'stock', 'store_id'])
    df.head(0).to_sql(config.LIDER_PRODUCT_NUMBER['table_lz'], engine, if_exists='append',index=False)
                    
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.LIDER_PRODUCT_NUMBER['table_lz'], null="") # null values become ''
    conn.commit()
    return

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

def get_status(store_id_in, store_name_in):
    result = []
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    
    url_1 = 'https://www.lider.cl/supermercado/includes/inventory/inventoryInformation.jsp?productNumbers='
    url_2 = '&useProfile=true&consolidate=false&storeIds='+str(store_id_in)
    
    products = get_product(str(store_id_in))
    stores_num = ''
    
    #limitador de productos para no ser bloqueado
    flag = 0
    
    for rp in products:
        try:    
            if flag == 0:
                stores_num = stores_num+str(rp[0])
            else:
                stores_num = stores_num+'%2C'+str(rp[0])
                    
            flag = flag+1
            if flag == 80:
                url = url_1 + stores_num + url_2
                    
                flag = 0
                stores_num = ''
            
                response = requests.request("POST", url, headers=headers)
                    
                page_content = BeautifulSoup(response.content, "html.parser")
                #print(page_content)
                data = json.loads(str(page_content))
                    
                for i in range(0, len(data)):
                    product_id = data[i]['productNumber']
                    stock = data[i]['stockLevel']
                    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        
                    data_array = {'store_id': store_id_in, 'product_id': product_id, 'stock': stock, 'insert_date': date}
                    result.append(data_array)
                insert_db(result)
                time.sleep(randrange(1,3))
                result = []
        except:
            pass

            
        #inserta el residuo de la division por 100      
    if flag != 0:
        url = url_1+stores_num+url_2
        response = requests.request("POST", url, headers=headers)
        page_content = BeautifulSoup(response.content, "html.parser")
        data = json.loads(str(page_content))
                
        for i in range(0, len(data)):
            product_id = data[i]['productNumber']
            stock = data[i]['stockLevel']
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      
            data_array = {'store_id': store_id_in, 'product_id': product_id, 'stock': stock, 'insert_date': date}
            result.append(data_array)
        insert_db(result)
            
    
    return
 
#main
stores = balance_cargas()

for store in stores:
    print(store['store_name'])
    get_status(store['store_id'], store['store_name'])

    
    
        
        
        