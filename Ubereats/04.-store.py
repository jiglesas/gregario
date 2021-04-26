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
from unidecode import  unidecode
from urllib.parse import unquote
import re
import string

cursor.execute("select id, url, id_city from scrapinghub.ubereats_category where active = '1' order by id")
listaCategoria = cursor.fetchall()

contador = 0
for categoria in listaCategoria:
    try:    
        id_category = categoria[0]
        url = categoria[1].replace("/city/","/category/")


        contador = contador + 1
        print(str(contador) +"/"+str(len(listaCategoria))+" "+url)
        
        id_city = categoria[2]

        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        main = soup.find_all("script")
        candidato = ""
        for a in main:
            if(len(a.contents)>0):
                if(len(candidato)<len(a.contents[0])):
                    candidato = a.contents[0]
        candidato = candidato.encode("utf-8").decode("unicode-escape").encode('iso-8859-1').decode("utf-8")
        candidato=candidato.replace("%5C\"", "'")
        
        data = json.loads(candidato)
        data = data["seoFeed"]
        data2 = []
        for d in data:
            data2 = data[d]
            if(data2["data"]==None):
                continue
            data2 = data2["data"]
            data2 = data2["elements"]

            for d2 in data2:
                if(d2["id"]=="categoryStores"):
                    data2 = d2
                    break

            data2 = data2["storesMap"]


            for store in data2:
                id = data2[store]["uuid"]
                name = data2[store]["title"]
                slug = data2[store]["slug"]

                urlTempList = soup.find_all("a", class_="", tabindex="-1")

                url2 = ""

                for urlTemp in urlTempList:
                    href = urlTemp.attrs
                    href = unidecode(unquote(href["href"]))
                    if(slug in href):
                        url2 = HOME+href

                if(url2 == ""):
                    for urlTemp in urlTempList:
                        href = urlTemp.attrs
                        href = unidecode(unquote(href["href"]))
                        a = "".join(e for e in slug if e.isalnum())
                        b = "".join(e for e in href if e.isalnum())
                        if(a in b):
                            url2 = HOME+href

                if(url2 == ""):
                    for urlTemp in urlTempList:
                        href = urlTemp.attrs
                        href = unidecode(unquote(href["href"]))
                        a = "".join(e for e in slug if e.isalpha())
                        b = "".join(e for e in href if e.isalpha())
                        if(a in b):
                            url2 = HOME+href

                if(url2 == ""):
                    for urlTemp in urlTempList:
                        href = urlTemp.attrs
                        href = unidecode(unquote(href["href"]))
                        a = "".join(e for e in slug if e in string.ascii_letters)
                        b = "".join(e for e in href if e in string.ascii_letters)
                        if(a in b):
                            url2 = HOME+href

                cursor.execute("select * from scrapinghub.ubereats_store where id = %s", (id, ))
                if len(cursor.fetchall())==1:
                    cursor.execute(
                        "UPDATE scrapinghub.ubereats_store SET name=%s, date_update=%s, url=%s, id_city=%s WHERE id=%s",
                        ( name, datetime.now(), url2, id_city, id)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO scrapinghub.ubereats_store(id, name, url, date_update, id_city, active) VALUES (%s, %s, %s, %s, %s, %s);", 
                        (id, name, url2, datetime.now(), id_city, 1)
                    )
                
                cursor.execute("select * from scrapinghub.ubereats_category_store where id_category = %s and id_store = %s", (id_category, id))
                if len(cursor.fetchall())==1:
                    cursor.execute(
                        "UPDATE scrapinghub.ubereats_category_store SET date_update=%s WHERE id_category = %s and id_store = %s",
                        (datetime.now(), id_category, id)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO scrapinghub.ubereats_category_store(id_category, id_store, date_update) VALUES (%s, %s, %s);", 
                        (id_category, id, datetime.now())
                    )         
            conn.commit()
    except Exception as e:
        print(e)
        try:
            cursor.execute(
                "INSERT INTO scrapinghub.ubereats_log_error(\"table\", id_reference, date_create, processed) VALUES (%s, %s, %s, %s);", ("categoria", id_category, datetime.now(), 0)
            )
            conn.commit()
        except Exception as e2:
            print(e2)
cursor.close()
conn.close()

print("termino")
