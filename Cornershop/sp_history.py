# -*- coding: utf-8 -*-
"""
run sp_history
"""
from datetime import datetime
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event


def run_history_sp():
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
        , connect_args={'options': '-csearch_path={}'.format('ctrl')})

    history_sp = "SELECT history.sp_load_model_functions();"

    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(history_sp)
        conn.commit()
        x = 1
    except: 
        x = 0
        
    return x

def insert_log(tipo, scraper):
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format('ctrl')})

    create_log = "INSERT INTO ctrl.scrapers_logs VALUES (clock_timestamp(), '"+str(tipo)+": SP "+str(scraper)+"');"

    #connection = engine.connect()
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(create_log)
    conn.commit()
    return

#main
temp = run_history_sp()
if temp == 1:
    insert_log('OK', 'history')
else:
    insert_log('ERROR', 'history')

