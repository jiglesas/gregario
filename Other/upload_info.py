# -*- coding: utf-8 -*-
"""
Script subir xls a tabla sql
"""
import pandas as pd
import config
import io
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

for i in range(0,1):
    
    #algo = pd.read_excel('pedidos_stores.xlsx', sheet_name='Hoja3', dtype=str)
    up_xls = pd.read_excel('agrosuper_homologado.xlsx', sheet_name='consolidado', dtype=str)
    print(up_xls)
    
    #homologacion marca
    df = pd.DataFrame(up_xls)
    df['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #d = {'true': True, 'false': False}
    #df['stores_status'] = df['store_status'].map(d)
    #df.store_status = df.store_status.astype(bool)
    #df = df.astype(str).remplace('.0', '')
    
    #homologacion stores
    #df[['store_cornershop_id', 'store_jumbo_id', 'store_rappi_market_id']] = df[['store_cornershop_id', 'store_jumbo_id', 'store_rappi_market_id']].astype(int)
    #df[['store_rappi_convenience_id', 'store_lider_id', 'store_tottus_id', 'store_pedidosya_id']] = df[['store_rappi_convenience_id', 'store_lider_id', 'store_tottus_id', 'store_pedidosya_id']].astype(int)

    #df['store_jumbo_id'].astype(float).sum().astype(int).astype(str)
    #df[['store_cornershop_id', 'store_jumbo_id', 'store_rappi_market_id', 'store_rappi_convenience_id', 'store_tottus_id', 'store_pedidosya_id']] = df[['store_cornershop_id', 'store_jumbo_id', 'store_rappi_market_id', 'store_rappi_convenience_id', 'store_tottus_id', 'store_pedidosya_id']].astype(float).sum().astype(int).astype(str)
    #df['store_cornershop_id'].astype(float).sum().astype(int).astype(str)
    #df['store_tottus_id'].astype(float).sum().astype(int).astype(str)
    
    
    '''
    df = df.rename(columns={'CODIGO_VENDEDOR': 'codigo_vendedor',
                                'CODIGO_OFICINA': 'codigo_oficina',
                                'CODIGO_CLIENTE': 'codigo_cliente',
                                'FECHA_VENTA': 'fecha_venta',
                                'FACTURA': 'factura',
                                'COSTO': 'costo',
                                'UMP': 'ump'}, index={'ONE': 'one'})
    
    '''

    
    #conexion a la bd
    engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
             , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})
    
    connection = engine.connect()
    
    df.head(0).to_sql(config.UPLOAD['table_lz'], engine, if_exists='append',index=False)
                        
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, config.UPLOAD['table_lz'], null="") # null values become ''
    conn.commit()

connection.close()