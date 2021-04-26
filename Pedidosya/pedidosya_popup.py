# -*- coding: utf-8 -*-
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 1

#url = 'https://www.pedidosya.cl/profile/getProductModal?id=17619685&discount=0&menusection=menu&sectionName=Bebidas&sectionTags=&bt=RESTAURANT&cr=&partnerId=112593'
def get_url(product_id, category_name, tipo, store_id):
    url = 'https://www.pedidosya.cl/profile/getProductModal?id='+str(product_id)+'&discount=0&menusection=menu&sectionName='+str(category_name)+'&sectionTags=&bt='+str(tipo)+'&cr=&partnerId='+str(store_id)
    return url

def insert_db(data_in):
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    df = pd.DataFrame(data_in)
    df.head(0).to_sql(config.PEDIDOS['table_lz'], engine, if_exists='append',index=False)
                        
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.PEDIDOS['table_lz'], null="") # null values become ''
    conn.commit()

def main():
    result = []
    result_product = balance_cargas()
    headers = { 
                              "Host": "www.pedidosya.cl",
                              "User-Agent": "Mozilla/5.0 (X11 Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0",
                              "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                              "Accept-Language": "en-US,en;q=0.5",
                              "Accept-Encoding": "gzip, deflate, br",
                              "Connection": "keep-alive",
                              "Referer": "https://www.pedidosya.cl/restaurantes/santiago",
                              "Upgrade-Insecure-Requests": "1",
                              "Pragma": "no-cache",
                              "Cache-Control": "no-cache",
                              "TE": "Trailers",
                              'Cookie': 'initialPage=Home; initialReferrer=; initialPagePath=https://www.pedidosya.cl/; _gac_UA-5817430-3=1.1612377000.CjwKCAiAsOmABhAwEiwAEBR0ZvHVTO0tisk_nJUOfwjNMix03mwk-tEp0_s2MJUQng-1DKaLoUxnOhoC3DUQAvD_BwE; amplitude_idpedidosya.cl=eyJkZXZpY2VJZCI6IjQ2ZmJjNjk2LWY0NmMtNDVjOS1iYTBmLWI4MzFlMDdkNTlmYyIsInVzZXJJZCI6bnVsbCwiZ2xvYmFsVXNlclByb3BlcnRpZXMiOnsiQ291bnRyeSBOYW1lIjoiQ2hpbGUiLCJDaXR5IE5hbWUiOiJTYW50aWFnbyIsIkxvZ2dlZCBJbiI6ZmFsc2UsIlVzZXIgSWQiOiIiLCJQbGF0Zm9ybSI6IldlYiIsIldoaXRlIExhYmVsIjoiIiwiSXMgV2lkZ2V0IjpmYWxzZX19; _gcl_aw=GCL.1612377001.CjwKCAiAsOmABhAwEiwAEBR0ZvHVTO0tisk_nJUOfwjNMix03mwk-tEp0_s2MJUQng-1DKaLoUxnOhoC3DUQAvD_BwE; _ga=GA1.2.1200963881.1611687562; _gid=GA1.2.738826538.1614715271; _gac_UA-68934388-1=1.1612377001.CjwKCAiAsOmABhAwEiwAEBR0ZvHVTO0tisk_nJUOfwjNMix03mwk-tEp0_s2MJUQng-1DKaLoUxnOhoC3DUQAvD_BwE; perseusRolloutSplit=5; dhhPerseusSessionId=1614862867522.325276915277767940.aacyqhfconj; dhhPerseusGuestId=1611687574389.776560906214771200.mhr4vwqa03c; ab._gd1614862891699=ab._gd1614862891699; ab._gd1614862891700=ab._gd1614862891700; py_searched_logged=true; sId=12; py_address_validated=%7B%22validated%22%3Atrue%2C%22lat%22%3A-33.4394198%2C%22lng%22%3A-70.6030165%2C%22city%22%3A%22Santiago%22%2C%22cityId%22%3A%222%22%2C%22street%22%3A%22ricardo%20lyon%22%2C%22doorNumber%22%3A%222050%22%2C%22number%22%3A%222050%22%7D; py_current_address=_address%3DRicardo+Lyon+2050%26_cityId%3D2%26_cityName%3DSantiago%26_countryId%3D2%26_lat%3D-33.4394198%26_lng%3D-70.6030165; py_current_city=_cityId%3D2%26_cityName%3DSantiago%26_countryId%3D2; cookies.js=1; previousEvent=Results%20Page; currentPage=Profile; currentEvent=Cart%20Started; amplitude_testpedidosya.cl=MC43MzE3NjE4ODI4NTI4ODA3; myOrders=; listingData={%22Profiles%20Viewed%22:25%2C%22Products%20Viewed%22:0%2C%22Position%22:0%2C%22Arrival%22:%22Direct%22%2C%22Restaurant%22:%22Lider%20-%20Irarr%C3%A1zaval%22%2C%22Filter%20Discount%22:false%2C%22Filter%20Payment%22:false%2C%22Filter%20Food%22:false%2C%22Filter%20Search%20bar%22:false%2C%22Sorting%22:false%2C%22Map%20Viewed%22:false%2C%22With%20Stamps%22:%22false%22%2C%22User%20Stamps%22:null%2C%22Stamps%20Needed%22:null%2C%22Free%20Order%22:null%2C%22Free%20Saldo%22:null}; ab.storage.sessionId.31acbe4c-735b-48b8-a130-e73f72257758=%7B%22g%22%3A%22ce9ee34d-afc9-3e6e-fb89-b135df90a317%22%2C%22e%22%3A1614864701438%2C%22c%22%3A1614862857139%2C%22l%22%3A1614862901438%7D; _tq_id.TV-81819090-1.cd2b=3827025507f65c9d.1611687565.0.1614862901..; dhhPerseusHitId=1614862914073.669723226484595800.s14azv238br; _pymkt=Campaign+Name%3DNot+set%26Campaign+Category%3DDirect%26Campaign+Source%3DDirect%26query%3Dnone%26creationDate%3D2021-03-03T23%3A25%3A14Z; JSESSIONID=59C34A49022F2B626F9D8A13FCAF888D; __cfduid=dd098541e70d839b1a68b4b5adbe6bbe31614863688; AWSALB=SxY6pw8AUiiGAGwH5bKxLMwrL7WfV2tG4T9iGA8BcFZE7LcFmZ6/nakI7Jhr37ZXf6393fqDv9YAdOR69VTDY2WHKD5x8hYrcxYpPHkn7dDz4Z9kFQWhrWWRuH6E; AWSALBCORS=SxY6pw8AUiiGAGwH5bKxLMwrL7WfV2tG4T9iGA8BcFZE7LcFmZ6/nakI7Jhr37ZXf6393fqDv9YAdOR69VTDY2WHKD5x8hYrcxYpPHkn7dDz4Z9kFQWhrWWRuH6E; __Secure-peya.sid=s%3A323a13e9-6345-406f-a23d-032ba3a7a734.T%2F0WyvB%2F3qHd7dEhUxREKe%2BmnQZGPi%2BrG7krfqpg5nA; __Secure-peyas.sid=s%3A62870444-c8ac-4895-89a0-569041dcc075.gyCWRk898ReYtiZIQhpbly%2BaLS%2F2lMglFP21PXQs0WU'
                              }
    
    #url = get_url(9891721, 'Bebidas', 'RESTAURANT', 122619)
    j = 0
    for i in result_product:
        print(str(j)+' de '+str(len(result_product)))
        j = j+1
        print('--------------------------------')
        category_name = i['name']
        category_name = category_name.replace(' ', '%20')
        url = get_url(i['product_id'], category_name, i['business_type'], i['partner_id'])
    
        page_response = requests.get(url, headers=headers, timeout=30)

        try:
            # Se parsea el texto a html.
            soup = BeautifulSoup(page_response.content, "html.parser")
            content = soup.find("section", {"class": "optionsContainer"})
            content = content.text
            content = content.replace('\n', ' ')
            data = {'partner_id': i['partner_id'], 'product_id': i['product_id'], 'content': content}
            print(data)
            result.append(data)
        except:
            print('ERROR:'+url)
            
        if(j == 1000 or j == 2000 or j == 10):
            insert_db(result)
            result = []
    insert_db(result)
    return

def balance_cargas():
    sublocal = []     
    #balance de carga en las VM
    num_vm = 6
    result_product = get_product()
    num_local = len(result_product)
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
            sublocal.append(result_product[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(result_product[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(result_product[k])
    elif id_vm == 4:
        for k in range(vm3, vm4):
            sublocal.append(result_product[k])
    elif id_vm == 5:
        for k in range(vm4, vm5):
            sublocal.append(result_product[k])
    else:
        for k in range(vm5, vm6):
            sublocal.append(result_product[k])
              
    return sublocal


def get_product():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    connection = engine.connect()
    
    result_products = connection.execute(config.PEDIDOS['query']).fetchall()
    connection.close()
    return result_products
    

if __name__ == "__main__":
    main()