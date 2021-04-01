# -*- coding: utf-8 -*-
"""
Log Scraper
"""
from datetime import datetime
import config
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event


def insert_log(tipo, scraper):
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format('ctrl')})

    create_log = "INSERT INTO ctrl.scrapers_logs VALUES (clock_timestamp(), '"+str(tipo)+": Scraper "+str(scraper)+"');"

    #connection = engine.connect()
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(create_log)
    conn.commit()
    return

#insert_log('end', 'cornershop_test')





