# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 21:00:34 2021

@author: newton
"""
from bs4 import BeautifulSoup
import time
import pandas as pd
from random import randrange
import config
import json
import threading
import io
import requests 
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event


def get_stores_ctrl(part_id):
    stores_out = []
    engine = create_engine('postgresql://jiglesias:jigle2020@34.69.175.136:5432/pimvault'
           , connect_args={'options': '-csearch_path={}'.format('ctrl')})
    connection = engine.connect()
    query = """
                    select 
                        value ->> 'section'
                    from 
                        ctrl.scrapers_activation 
                    where 
                        retail = 'Pedidosya' and data_type = 'Store' and activate= true 
                        and value ->> 'partner_id' = '"""+part_id+"""'"""
    print(query)
    stores = connection.execute(query).fetchall()
    for rc in stores:
        var_temp = json.dumps(rc[0])
        stores_in = json.loads(var_temp)
        #section = stores_in['section']
        
        stores_in = stores_in.replace('[', ' ')
        stores_in = stores_in.replace(']', ' ')
        stores_in = stores_in.replace("'", ' ')
        stores_in = stores_in.replace(' ', '')
        #print(stores_in)
        # setting the maxsplit parameter to 1, will return a list with 2 elements!
        x = stores_in.split(",")
        for i in range(0, len(x)):
            stores_out.append(x[i])


        #print(len(x))
        #print(stores_out)


    connection.close()
    return stores_out


algo = get_stores_ctrl("194102")

for a in algo:
    text = 'asdsdasd'+a+'asdsadsd'
    print(text)


