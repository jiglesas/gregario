API = "https://www.ubereats.com/api/getStoreV1"

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

def getsubsectionUuid(data, menuItemUuid):
    for subsectionUuid in data["subsectionsMap"]:
        subsection = data["subsectionsMap"][subsectionUuid]
        for itemUuids in subsection["itemUuids"]:
            if(itemUuids==menuItemUuid):
                return subsectionUuid

# cursor.execute("select id, url from scrapinghub.ubereats_store where active = '1'")
cursor.execute("select a.* from scrapinghub.ubereats_store a left join scrapinghub.ubereats_product b on a.id = b.id_store where b.id_store is null and a.active='1'")

contador = 0
listaLocal = cursor.fetchall()
for local in listaLocal:
    try:
        id_store = local[0]
        url = API

        
        contador = contador + 1
        print(str(contador) +"/"+str(len(listaLocal))+" "+id_store)

        data = {"storeUuid": id_store}
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'x-csrf-token': 'x'}
        x = requests.post(url, data=json.dumps(data), headers=headers)
        data = x.json()
        data = x.json()["data"]
        storeUuid = id_store
        for sectionUuid in data["sectionEntitiesMap"]:
            sectionEntitiesMap = data["sectionEntitiesMap"][sectionUuid]
            for menuItemUuid in sectionEntitiesMap:
                clasificacion = sectionEntitiesMap[menuItemUuid]
                subsectionUuid = getsubsectionUuid(data, menuItemUuid)
                if(subsectionUuid==None): 
                    continue
                cursor.execute("select * from scrapinghub.ubereats_product where menu_item_uuid = %s and section_uuid = %s and subsection_uuid = %s and store_uuid = %s", (menuItemUuid, sectionUuid, subsectionUuid, storeUuid))
                a = cursor.fetchall()
                if len(a)==0:
                    cursor.execute(
                        "INSERT INTO scrapinghub.ubereats_product(id_store, menu_item_uuid, section_uuid, subsection_uuid, store_uuid, date_update, active) VALUES (%s, %s, %s, %s, %s, %s, %s);", 
                        (id_store, menuItemUuid, sectionUuid, subsectionUuid, storeUuid, datetime.now(), 1)
                    ) 
        cursor.execute(
            "UPDATE scrapinghub.ubereats_store SET json=%s WHERE id=%s",
            ( json.dumps(data), id_store)
        )

        cursor.execute("delete from scrapinghub.ubereats_log_error where \"table\" = 'local' and id_reference = %s", (id_store,))

    except Exception as e:
        cursor.execute("delete from scrapinghub.ubereats_log_error where \"table\" = 'local' and id_reference = %s", (id_store,))
        cursor.execute(
            "INSERT INTO scrapinghub.ubereats_log_error(\"table\", id_reference, date_create, processed) VALUES (%s, %s, %s, %s);", ("local", id_store, datetime.now(), 0)
        )
    conn.commit()
cursor.close()
conn.close()


#https://www.scrapinghub.com/arica/food-delivery/botilleria-%E2%80%9C-don-omar-el-senor-de-la-noche%E2%80%9D/-2Jr67-LSt2yOAnmVu98Rw/f0422b33-3cdf-4025-9d2d-c5f61e1ad8ce/7e12b8f3-0bcf-55dd-81ca-8fd953501d6e/0ccd7832-589d-4688-87b1-5a52c2abc9ce?ps=1

