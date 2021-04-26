HOME = "https://www.ubereats.com"
REGION = "/cl/location"


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
r = requests.get(HOME+REGION)
soup = BeautifulSoup(r.text, 'lxml')
main = soup.find_all(id='main-content')[0]

listaCiudadTemp = main.contents[1].contents

listaRegion = []
for temp in listaCiudadTemp:
    if len(temp.contents)==2:
        region = temp.contents[0].contents[0].contents[0].contents[0]
        id = region.attrs["href"]
        nombre = region.contents[0]
        cursor.execute("select * from scrapinghub.ubereats_region where id = %s", (id, ))
        if len(cursor.fetchall())==1:
            cursor.execute(
                "UPDATE scrapinghub.ubereats_region SET name=%s, date_update=%s, url=%s WHERE id=%s",
                ( nombre, datetime.now(), HOME+id, id)
            )
        else:
            cursor.execute(
                "INSERT INTO scrapinghub.ubereats_region(id, name, url, date_update, active) VALUES (%s, %s, %s, %s, %s);", 
                (id, nombre, HOME+id,datetime.now(), 1)
            )
conn.commit()
cursor.close()
conn.close()