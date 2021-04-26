API = "https://www.ubereats.com/api/getMenuItemV1?localeCode=cl"

import psycopg2

try:
    # conn = psycopg2.connect(dbname='prueba', user='postgres', host='127.0.0.1', password='adolfo', port="5433")
    conn = psycopg2.connect(dbname='pimvault', user='adiaz', host='34.69.175.136', password='adiaz2021', port="5432")
    cursor = conn.cursor()
except Exception as e:
    print(e)

from datetime import datetime
import json
import requests
from bs4 import BeautifulSoup
from threading import Thread, Lock
mutex = Lock()

NumeroThread = 250

def GetDetalleproduct(product):
    id_store = product[0]
    menu_item_uuid = product[1]
    section_uuid = product[2]
    subsection_uuid = product[3]
    store_uuid = product[4]
    url = API
    
    data = {
        "menuItemUuid":menu_item_uuid
        ,"sectionUuid":section_uuid
        ,"subsectionUuid":subsection_uuid
        ,"storeUuid":store_uuid
    }
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'x-csrf-token': 'x2'}
    try:
        x = requests.post(url, data=json.dumps(data), headers=headers, timeout=25)
    except Exception as e:
        print("timeout")
        return

    data = x.json()
    data = data["data"]
    _json = json.dumps(data)

    price = 0
    try:
        price = data["price"]/100
    except Exception as e:
        print("error price")
    
    imageurl = ""
    try:
        imageUrl = data["imageUrl"]
    except Exception as e:
        print("error imageUrl")
    
    name = ""
    try:
        name = data["title"]
    except Exception as e:
        print("error imageUrl")
    
    

    mutex.acquire()
    try:
        cursor.execute(
            "update scrapinghub.ubereats_product set price = %s, name = %s, imageurl = %s, json = %s where menu_item_uuid=%s and section_uuid=%s and subsection_uuid=%s and store_uuid=%s", 
            (price, name, imageurl, _json, menu_item_uuid, section_uuid, subsection_uuid, store_uuid)
        )
    finally:
        mutex.release()

# cursor.execute("select id_store, menu_item_uuid, section_uuid, subsection_uuid, store_uuid from scrapinghub.ubereats_product where active = '1'")
cursor.execute("select id_store, menu_item_uuid, section_uuid, subsection_uuid, store_uuid from scrapinghub.ubereats_product where active = '1' and json is null")
contador = 0
listaLocal = cursor.fetchall()

t = []
for product in listaLocal:
    if(len(t)>=NumeroThread):
        for x in range(0, len(t)):
            t[x].start()
        for x in range(0, len(t)):
            t[x].join()
        t = []
        conn.commit()



    try:
        id_store = product[0]
        menu_item_uuid = product[1]
        
        contador = contador + 1
        print(str(contador) +"/"+str(len(listaLocal))+" "+id_store)        
        
        t.append(Thread(target=GetDetalleproduct, args=(product,)))

        
    except Exception as e:
        mutex.acquire()
        try:
            cursor.execute(
                "INSERT INTO scrapinghub.ubereats_log_error(\"table\", id_reference, date_create, processed) VALUES (%s, %s, %s, %s);", ("detalle_product", menu_item_uuid, datetime.now(), 0)
            )
        finally:
            mutex.release()
        conn.commit()
        
        

for x in range(0, len(t)):
    t[x].start()
for x in range(0, len(t)):
    t[x].join()

conn.commit()

cursor.close()
conn.close()



