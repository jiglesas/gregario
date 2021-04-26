import pandas as pd
import config
import threading
import json
import log
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 4
result = []
#api sample
#https://www.tottus.cl/api/product-search/by-category-slug?slug=carnes-cat0101&sort=recommended_web&channel=Regular_Delivery_RM_3&page=1&perPage=1000

def get_url(slug, channel):
    link = 'https://www.tottus.cl/api/product-search/by-category-slug?slug='+slug+'&sort=recommended_web&channel='+channel+'&page=1&perPage=1000'
    #print(link)
    return link

def get_stores(slug, channel):
    url = get_url(slug, channel)
    response = requests.get(url)
    
    if response.status_code == 200:
        data = json.loads(response.text)
        
        product_len = len(data['results'])
        for i in range(0, product_len):
            #print(data['results'][i]['key'])
            store_id = channel
            category_slug = slug 
            _id = product_id = data['results'][i]['id']
            product_id = data['results'][i]['productId']
            sku = data['results'][i]['id']
            product_key = data['results'][i]['key']
            product_slug = data['results'][i]['slug']
            product_name = data['results'][i]['name']
            product_description = data['results'][i]['description']
            product_description = product_description.replace('\n', ' ')
            product_description = product_description.replace('\t', ' ')
            ean = data['results'][i]['attributes']['ean']
            estado = data['results'][i]['attributes']['estado']
            brand = data['results'][i]['attributes']['marca']
            normal_price = data['results'][i]['prices']['regularPrice']
            offer_price = data['results'][i]['prices']['discountPrice']

            package = data['results'][i]['attributes']['formato']

            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            product_data = {'id': _id,'store_id': store_id, 'category_slug': category_slug, 'product_id': product_id, 'sku': sku, 'product_key': product_key, 'product_slug': product_slug, 
                            'product_name': product_name, 'product_description': product_description, 'ean': ean, 'brand': brand,'create_date': date
                            , 'normal_price': normal_price, 'status': estado, 'package': package,'offer_price': offer_price }
            result.append(product_data)
            
    return

def balance_cargas():
    sublocal = []
        
    #balance de carga en las VM
    num_vm = 4
    num_local = len(result_stores)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = num_local
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(result_stores[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(result_stores[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(result_stores[k])
    else:
        for k in range(vm3, vm4):
            sublocal.append(result_stores[k])
              
    return sublocal

#encabezado log
#log.insert_log('Start', 'Tottus-'+str(id_vm))
print('HORA INICIO: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

#conexion a la bd
engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()   


result_category = connection.execute(config.TOTTUS_CATEGORY['query_category_prod']).fetchall()
result_stores = connection.execute(config.TOTTUS_STORES['query_stores_prod']).fetchall()
#get_stores(slug, channel)
locales = balance_cargas()
#print(locales)

for i in locales:
    json_temp = json.dumps(i[0])
    store_in = json.loads(json_temp)
    for j in result_category:
        var_temp = json.dumps(j[0])
        category_in = json.loads(var_temp)
        get_stores(category_in['subcategory_slug'], store_in['store_id'])

    
df = pd.DataFrame(result)  
df = df.reindex(columns=['id','store_id','category_slug','product_id','sku','product_key','product_slug','product_name','product_description','ean','brand', 'create_date', 'normal_price', 'status', 'package','offer_price'])

df.head(0).to_sql(config.TOTTUS_PRODUCT_DETAIL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.TOTTUS_PRODUCT_DETAIL['table_lz'], null="") # null values become ''
conn.commit()

connection.close()
print('HORA Fin: '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
#log.insert_log('End', 'Tottus-'+str(id_vm))


