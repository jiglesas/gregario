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

cursor.execute("select id, url from scrapinghub.ubereats_city where active = '1' order by id")
listacity = cursor.fetchall()
contador = 0
for region in listacity:
    try:
        id_city = region[0]
        url = region[1].replace("/city/","/category/")

        contador = contador + 1
        print(str(contador) +"/"+str(len(listacity))+" "+url)

        r = requests.get(url.strip())
        soup = BeautifulSoup(r.text, 'lxml')

        main = soup.find_all("script")
        candidato = ""
        for a in main:
            if(len(a.contents)>0):
                if(len(candidato)<len(a.contents[0])):
                    candidato = a.contents[0]
        candidato = candidato.encode("utf-8").decode("unicode-escape").encode('iso-8859-1').decode("utf-8")
        candidato=candidato.replace("%5C\"", "'")
        data = json.loads(candidato)["categories"]
        for index in data:
            data2 = data[index]["data"]["links"]
            for data3 in data2:
                a = 2
                id = data3["href"]
                name = data3["text"]
                cursor.execute("select * from scrapinghub.ubereats_category where id = %s", (id, ))
                if len(cursor.fetchall())==1:
                    cursor.execute(
                        "UPDATE scrapinghub.ubereats_category SET name=%s, date_update=%s, url=%s, id_city=%s WHERE id=%s",
                        ( name, datetime.now(), HOME+id, id_city, id)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO scrapinghub.ubereats_category(id, id_city, name, url, date_update, active) VALUES (%s, %s, %s, %s, %s, %s);", 
                        (id, id_city, name, HOME+id, datetime.now(), 1)
                    )
        conn.commit()
    except Exception as e:
        print(e)
        try:
            cursor.execute(
                "INSERT INTO scrapinghub.ubereats_log_error(table, id_reference, date_create, processed) VALUES (%s, %s, %s, %s);", ("city", id_city, datetime.now(), 0)
            )
            conn.commit()
        except Exception as e2:
            print(e2)        
cursor.close()
conn.close()


