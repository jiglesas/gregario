from selenium import webdriver
import time
import pandas as pd
import config
import io
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument('start-maximized')
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get('https://www.jumbo.cl/')

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
time.sleep(2)

result_stores = connection.execute(config.JUMBO_STORES['query_stores']).fetchall()
print(result_stores)

for i in range(0, len(result_stores)):
    print(result_stores[i]['name'])

    button_temp = driver.find_element_by_class_name('delivery-icon-content')
    button_temp.click()
    time.sleep(2)
    
    retiro = driver.find_elements_by_class_name('delivery-type-tab-title')[1]
    retiro.click()
    
    result = []
    names = []
    
    locales = len(driver.find_elements_by_class_name('store-item-address'))
    #print(locales)
    for i in range(0, locales):
        name = driver.find_elements_by_class_name('store-item-title')[i].text
        #print(name)
        adress = driver.find_elements_by_class_name('store-item-address')[i].text
        #print(adress)
        data = {'id': i, 'name': name, 'adress': adress, 'activated': True}
        result.append(data)
    

df = pd.DataFrame(result)
'''
df.head(0).to_sql(config.JUMBO_STORES['table_lz'], engine, if_exists='append',index=False)
                
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, config.JUMBO_STORES['table_lz'], null="") # null values become ''
conn.commit()
'''


#seleccionar local
local = driver.find_elements_by_class_name('store-item-wrap')[1]

largo = len(driver.find_elements_by_class_name('store-item-wrap'))
print(largo)
for i in range(0,largo):
    store_name = driver.find_elements_by_class_name('store-item-title')[i].text
    #print(store_name)
#direccion = driver.find_elements_by_class_name('store-item-address')[1].text
#store_name = driver.find_elements_by_class_name('store-item-title')[1].text


local.click()

time.sleep(2)
radio_button = driver.find_elements_by_css_selector('input.input-radio-button')[1]
radio_button.click()

time.sleep(2)
confirm = driver.find_elements_by_css_selector('button.delivery-selector-btn:nth-child(2)')[0].click()

connection.close()

#/html/body/div[1]/div/header/div[1]/div/div[2]/div[3]/div/div/button