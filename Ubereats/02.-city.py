HOME = "https://www.ubereats.com"

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

cursor.execute("select id, url from scrapinghub.ubereats_region where active = '1' order by id")

listaRegion = cursor.fetchall()
contador = 0
for region in listaRegion:
    try:
        id_region = region[0]
        url = region[1]

        contador = contador + 1
        print(str(contador) +"/"+str(len(listaRegion))+" "+url)

        r = requests.get(url.strip())
        soup = BeautifulSoup(r.text, 'lxml')
        main = soup.find_all(id='main-content')[0]
        main = main.find_all("a")
        for city in main:
            id = city.attrs["href"]
            if(id.find("/cl/city/")==-1):
                continue
            name = city.contents[0].contents[0].contents[0].contents[0]
            cursor.execute("select * from scrapinghub.ubereats_city where id = %s", (id, ))
            if len(cursor.fetchall())==1:
                cursor.execute(
                    "UPDATE scrapinghub.ubereats_city SET name=%s, date_update=%s, url=%s, id_region=%s WHERE id=%s",
                    ( name, datetime.now(), HOME+id, id_region, id)
                )
            else:
                cursor.execute(
                    "INSERT INTO scrapinghub.ubereats_city(id, id_region, name, url, date_update, active) VALUES (%s, %s, %s, %s, %s, %s);", 
                    (id, id_region, name, HOME+id, datetime.now(), 1)
                )
        conn.commit()
    except Exception as e:
        print(e)
        try:
            cursor.execute(
                "INSERT INTO scrapinghub.ubereats_log_error(table, id_reference, date_create, processed) VALUES (%s, %s, %s, %s);", ("region", id_region, datetime.now(), 0)
            )
            conn.commit()
        except Exception as e2:
            print(e2)
cursor.close()
conn.close()


# r = requests.get(HOME+REGION)
# soup = BeautifulSoup(r.text, 'lxml')
# main = soup.find_all(id='main-content')[0]


