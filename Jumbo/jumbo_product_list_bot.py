from selenium import webdriver
import time
from bs4 import BeautifulSoup
import pandas as pd
import config
from datetime import datetime
import io
import json
from sqlalchemy import create_engine, Column, MetaData, Table, DateTime, String, Integer, ForeignKey, BIGINT,TEXT,FLOAT,inspect, event

id_vm = 1

def balance_cargas():
    sublocal = []
        
    #balance de carga en las VM
    num_vm = 4
    num_local = len(result_stores)
    #temporal para pruebas
    #num_local = num_local//8
   
    vm0 = 0
    vm1 = num_local//num_vm
    vm2 = (num_local//num_vm) * 2
    vm3 = (num_local//num_vm) * 3
    vm4 = num_local
    
    if id_vm == 1:
        for k in range(vm0, vm1):
            sublocal.append(result_stores[k])
    elif id_vm == 2:
        for k in range(vm1, vm2):
            sublocal.append(result_stores[k])
    elif id_vm == 3:
        for k in range(vm2, vm3):
            sublocal.append(result_stores[k])
    else:
        for k in range(vm3, vm4):
            sublocal.append(result_stores[k])
              
    return sublocal


url_root = 'https://www.jumbo.cl'
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument('start-maximized')
options.add_argument("--no-sandbox");
options.add_argument("--disable-extensions");
options.add_argument("--dns-prefetch-disable");
options.add_argument("--disable-gpu");
#options.add_argument("--headless");
#options.setPageLoadStrategy(PageLoadStrategy.NORMAL);
driver = webdriver.Chrome('./chromedriver.exe', options=options)
driver.get('https://www.jumbo.cl/')

engine = create_engine('postgresql://'+config.DATABASE_CONFIG['user']+':'+config.DATABASE_CONFIG['password']+'@'+config.DATABASE_CONFIG['host']+':'+config.DATABASE_CONFIG['port']+'/'+config.DATABASE_CONFIG['dbname']
         , connect_args={'options': '-csearch_path={}'.format(config.DATABASE_CONFIG['schema'])})

connection = engine.connect()
time.sleep(2)

info = []
result_stores = connection.execute(config.JUMBO_STORES['query_stores_pill']).fetchall()
result_categories = connection.execute(config.JUMBO_CATEGORY['query_category_prod']).fetchall()
#print(result_stores)
j = 0

print(result_stores)
#balance de cargas
locales = balance_cargas()

#for j in range(0, len(result_stores)):
for rs in locales:
    #json_temp = json.dumps(rs[0])
    #stores_in = json.loads(json_temp)
    #print(stores_in['store_name'])
    
    #store_in = stores_in['store_name']
    store_in = rs[0] 
    
    print(store_in)
    time.sleep(1)
    if j == 0:
        button_temp = driver.find_element_by_class_name('new-header-change-adress-button')
    else:
        time.sleep(1)
        button_temp = driver.find_element_by_xpath('//*[@id="root"]/div/header/div[3]/div/div[2]/div[1]/div/div/div/span')
    
    button_temp.click()
    time.sleep(2)
    
    retiro = driver.find_elements_by_class_name('delivery-type-tab-title')[1]
    retiro.click()
    
    #seleccionamos local
    largo = len(driver.find_elements_by_class_name('store-item-wrap'))
    #print(largo)
    for i in range(0,27):
        try:
            time.sleep(1)
            store_name = driver.find_elements_by_class_name('store-item-title')[i].text
        except:
            store_name = '0'
            #print('error')
        if store_name == store_in:
            local = driver.find_elements_by_class_name('store-item-wrap')[i]
            
            local.click()
            
            time.sleep(2)
            #radio_button = driver.find_elements_by_css_selector('input.input-radio-button')[i]
            radio_button = driver.find_element_by_xpath('//*[@id="list-stores"]/li['+str(i+1)+']/span[3]')
           
            radio_button.click()
            
            time.sleep(2)
            confirm = driver.find_elements_by_css_selector('button.delivery-selector-btn:nth-child(2)')[0].click()
            time.sleep(2)

            
            
            for wp in result_categories:
                #print(wp)
                var_temp = json.dumps(wp[0])
                categorys_in = json.loads(var_temp)
                category_in = categorys_in['category_url']
                
                driver.get(url_root + category_in)
                time.sleep(4)
                content = driver.page_source.encode("utf-8").strip()
                soup = BeautifulSoup(content, "lxml")
                
                #iteracion por cada pagina de la seccion
                cant_pag = soup.findAll("button", class_= "page-number")
                for pag in cant_pag:
                    #print(pag.text)
                    path_pro = url_root + category_in+'?page='+pag.text
                    driver.set_page_load_timeout(30)
                    driver.get(path_pro)

                    time.sleep(3)
                    if path_pro == driver.current_url:
                        content = driver.page_source.encode("utf-8").strip()
                        soup = BeautifulSoup(content, "lxml")
                        
                        #captura de datos de los contedores
                        for div in soup.findAll("li", {"class": "shelf-item"}):
                            name = div.find("a", {"class": "shelf-product-title"}).text
                            url = div.find("a" , {"class": "shelf-wrap-image"} )['href']
                            brand = div.find("h3", {"class": "shelf-product-brand"}).text
                            format_ = div.find("span", {"class": "shelf-single-unit"}).text
                            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                            if div.find("span", {"class": "product-add-cart-text"}):
                                stock = True
                            else:
                                stock = False
                                
                            if div.find("div", {"class": "product-single-price-container"}):
                                price = div.find("div", {"class": "product-single-price-container"}).text
                                offer = False
                            elif div.find("span", {"class": "price-product-best"}):
                                price = div.find("span", {"class": "price-product-best"}).text
                                offer = True
                            else:
                                price = 'error'
                                offer = False
                            
                            product_list_jumbo = {'store_name': store_name,'category_url': category_in, 'product_url': url, 'name': name, 'brand': brand,
                                                  'formato': format_, 'price': price, 'creation_date': date, 'stock': stock, 'offer': offer}
                       
                            info.append(product_list_jumbo)     
                    
                    else:
                        time.sleep(1)
        else:
            pass
            
           
    df_cat = pd.DataFrame(info)
    df_cat = df_cat.reindex(columns=['store_name','category_url','product_url','name','brand','formato','price','creation_date','stock','offer'])

    df_cat.head(0).to_sql(config.JUMBO_PRODUCT_LIST['table_lz'], engine, if_exists='append',index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df_cat.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, config.JUMBO_PRODUCT_LIST['table_lz'], null="") # null values become ''
    conn.commit() 
    info = []
    j = j + 1
    time.sleep(2)

connection.close
driver.close()
 
    
