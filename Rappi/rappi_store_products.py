from selenium import webdriver
from seleniumrequests import Chrome
from bs4 import BeautifulSoup
import time
import pandas as pd
import config
import json
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

#api sample
#https://services.rappi.cl/api/sidekick/base-crack/principal?lng=-70.6211525&lat=-33.4588663
#https://services.rappi.cl/api/auth/guest_access_token
#https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/900024780

#funcion para crear el token (dura 1 hra el token)
def get_token():
    x = requests.post('https://services.rappi.cl/api/auth/guest_access_token')
    data = json.loads(x.text)
    token = data['access_token']
    return token

def get_url(id):
    url = 'https://services.rappi.cl/api/ms/web-proxy/restaurants-bus/store/id/'+id
    return url

result = []    
token = get_token()
id_in = '900018909'

url = get_url(id_in)
hed = {'Authorization': 'Bearer ' + token}
parameters = {"store_type": "restaurant", "lat": 0, "lng": 0}
y = requests.post(url, headers=hed, json=parameters)

data = json.loads(y.text)

store_id = data['store_id']
super_store_id = data['super_store_id']
brand_name = data['brand_name']
store_type = data['store_type']
name = data['name']
address = data['address']
zone_id = data['zone_id']
lat = data['location'][1]
lng = data['location'][0]
tag = lat = data['tags'][0]['name']

data = {'store_id': store_id, 'super_store_id': super_store_id, 'brand_name': brand_name, 'store_type': store_type,
        'name': name, 'address': address, 'zone_id': zone_id, 'lng': lng, 'lat': lat, 'tag': tag}

result.append(data)
print(data)




    
