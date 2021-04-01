from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import config
import json
import io
from datetime import datetime
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

#https://cornershopapp.com/api/v2/branches/9

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
result = []
#18539 ultimo local hasta el 14/12/2020
#errores con BRazil dps del numero 6467 por eso se excluyen revisar
for i in range(16395,20000):
    #print (i)
    api_url = 'https://cornershopapp.com/api/v2/branches/'+str(i)
    print(api_url)
    driver.get(api_url)
    time.sleep(1)
    
    content_api = driver.page_source.encode("utf-8").strip()
    soup_api = BeautifulSoup(content_api, "html.parser")
    try:
        api_content = soup_api.find('pre').text
    except :
        print('error de api')
        
    data = json.loads(api_content)

    try:
        store_id = data['branch']['id']
        store_name = data['branch']['name']
        adress = data['branch']['address']
        organization_id = data['branch']['store_id']
        organization_name = data['branch']['store_name']    
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        country = data['branch']['country']
        
        info = {'store_id': store_id, 'organization_id': organization_id, 'store_name': store_name, 'organization_name': organization_name, 'adress': adress, 'country': country}
        if country == 'CL':
            result.append(info)
    except:
        print('error')

df = pd.DataFrame(result)
    
df.head(0).to_sql(config.CORNERSHOP_GLOBAL['table_lz'], engine, if_exists='append',index=False)
                    
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.CORNERSHOP_GLOBAL['table_lz'], null="") # null values become ''
conn.commit()
connection.close()
driver.close()