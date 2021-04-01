import urllib.request
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import unidecode
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 4
def balance_cargas():
    sublocal = []     
    #balance de carga en las VM
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

def get_stores():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_products = connection.execute(config.PY_PRODUCT['query_hy']).fetchall()
    connection.close()
    return result_products

def insert_db(data_in, lz):
    if lz == 1:
        table_lz = 'table_lz'
        columns = {'store_id', 'product_id', 'product_name', 'product_description', 'price', 'category_id', 'category_name', 'create_date'}
    else:
        table_lz = 'table_error'
        columns = {'store_id', 'url'}
        
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    df = pd.DataFrame(data_in)
    df = df.reindex(columns=columns)
    df.head(0).to_sql(config.PY_PRODUCT[table_lz], engine, if_exists='append',index=False)
                        
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.PY_PRODUCT[table_lz], null="") # null values become ''
    conn.commit()
    
def main():
    result = []
    data_error = []
    headers = { 
                              "Host": "www.pedidosya.cl",
                              "User-Agent": "Mozilla/5.0 (X11 Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0",
                              "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                              "Accept-Language": "tr,tr-TR,en-US,en;q=0.8",
                              "Accept-Encoding": "gzip, deflate, br, sdch",
                              "accept-charset": "cp1254,ISO-8859-9,utf-8;q=0.7,*;q=0.3",
                              "Connection": "keep-alive",
                              "Referer": "https://www.pedidosya.cl/restaurantes/santiago",
                              "Upgrade-Insecure-Requests": "1",
                              "Pragma": "no-cache",
                              "Cache-Control": "no-cache",
                              "TE": "Trailers",
                              }
    total_stores = balance_cargas()

    k = 0
    #for rs in total_stores:
    for z in range(0,1):
        print(str(k)+' de '+str(len(total_stores)))
        k = k+1
        
        #url = rs['url']
        #url = url.replace('"', '')
        url = 'https://www.pedidosya.cl/restaurantes/arica/papa-johns-pizza-arica-sur-menu'
        page_response = requests.get(url, headers= headers, timeout=30, allow_redirects=False)

        
        
        soup = BeautifulSoup(page_response.content.decode('utf-8', 'ignore'), "html.parser")
        container = soup.find_all("div", {"class": "sectionContainer"})
        #print(len(container))
        
        if len(container) > 0:
            subcat = soup.find_all("section", {"class": "menuSectionContainer"})
            #print(len(subcat))
            for i in range(0,len(subcat)):
                #print(subcat[0].text)
                title = subcat[i].find("div", {"class": "sectionTitle"}).text
                title = title.replace("\n", "")
                title = title.replace("\t", "")
                title_id = subcat[i].find("h3")['data-id']
                title_id = title_id.replace("\n", "")
                title_id = title_id.replace("\t", "")
                title_id = title_id.replace("\r", "")
                product = subcat[i].find_all("li", {"class": "peyaCard"})
                #print(title_id)
                for j in range(0, len(product)):
                    item_id = product[j]['data-id']
                    item_id = item_id.replace("\n", "")
                    item_id = item_id.replace("\t", "")
                    item_id = item_id.replace("\r", "")
                    try:
                        item_name = product[j].find("h4", {"class": "productName"}).text
                    except:
                        item_name = 'error'
                    item_name = item_name.replace("\n", "")
                    item_name = item_name.replace("\t", "")
                    item_name = item_name.replace("\r", "")
                    try:
                        item_description = product[j].find("p", {"class": "product-description"}).text
                    except:
                        item_description = 'error'
                    item_description = item_description.replace("\n", "")
                    item_description = item_description.replace("\t", "")
                    item_description = item_description.replace("\r", "")
                    
                    item_price = product[j].find("div", {"class": "price"}).text
                    item_price = item_price.replace("\n", "")
                    item_price = item_price.replace("\t", "")
                    item_price = item_price.replace("\r", "")
                    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(item_name)
                    
                    #data = {"store_id": str(rs['store_id']), "product_id": str(item_id), "product_name": str(item_name), "product_description": str(item_description),
                    #        "price": str(item_price), "category_id": str(title_id), "category_name": str(title), "create_date": str(create_date)}
                    #result.append(data)
                    
                #schedule = soup.find_all("section", {"class": "tab-panel"})
                #print(schedule)
                        
        else:
            #data_error = { 'store_id' :rs['store_id'], 'url': rs['url']}
            pass
            
            
    #insert_db(result, 1)
    #insert_db(data_error, 0)
    return

if __name__ == "__main__":
    main()
