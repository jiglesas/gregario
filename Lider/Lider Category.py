from bs4 import BeautifulSoup
import requests 
import pandas as pd
# pip install psycopg2
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event
import gc
import io


import config

result = []

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()

# Se inserta la página web donde se generará el scraper.
page_link = config.LIDER_CATEGORY['url']

# Se entrega el agente de browser o entrega error de acceso.
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
# Se hace un request a la página y queda todo almacenado en la variable.
page_response = requests.get(page_link, headers=headers, timeout=30)

# Se parsea el texto a html.
page_content = BeautifulSoup(page_response.content, "html.parser")
#we use the html parser to parse the url content and store it in a variable.
ul_content = page_content.find_all("ul", {"class": "subcats"})

for ul in ul_content:
    
    a_content = ul.find_all('a', href=True)
    
    category_array = []
    
    for a in a_content:
        if not 'class' in a.attrs:
            category_array.append(a['href'])
        
    
    if len(category_array) != 1:
        df = pd.DataFrame(category_array, columns=['category_url'])
        df.columns = map(str.lower, df.columns)
            
        df.head(0).to_sql(config.LIDER_CATEGORY['table_lz'], engine, if_exists='append',index=False) #truncates the table
    
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output, config.LIDER_CATEGORY['table_lz'], null="") # null values become ''
        conn.commit()
    
        
gc.collect()

connection.close()
