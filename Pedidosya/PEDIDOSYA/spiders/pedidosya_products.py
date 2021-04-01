import scrapy
from scrapy.shell import inspect_response
import psycopg2
from psycopg2 import Error
import ast
from sqlalchemy import create_engine


from .pedidosya_base import PedidosyaBaseSpider

class PedidosyaProductsSpider(PedidosyaBaseSpider):
    name = 'pedidosyacl_products'
   
    def start_requests(self):
        insert_log('Start', 'Pedidos Ya')
        self.create_connection()
        # Que chequee los mercados active
        yield scrapy.Request('http://pedidosya.cl/')

    def parse(self, item):
        ## Get partner/section/product to update
        #self.cursor.execute("""SELECT partner_id, business_type, sections FROM pedidosyacl_partners WHERE active""")

        engine = create_engine('postgresql://jiglesias:jigle2020@34.69.175.136:5432/pimvault'
                , connect_args={'options': '-csearch_path={}'.format('ctrl')})
        connection = engine.connect()
        query = """
                    select
                        value ->> 'partner_id' as "partner_id"
                        , value ->> 'business_type' as "business_type"
                        , value ->> 'section' as "sections"
                    from 
                        ctrl.scrapers_activation 
                    where 
                        retail = 'Pedidosya' and data_type = 'Store' and activate= true
                        and value ->> 'business_type' = 'GROCERIES'
                """
            
        selected_partners = connection.execute(query).fetchall()
        print(selected_partners)




        #selected_partners= self.cursor.fetchall()

        #self.cursor.execute("""SELECT section_id FROM pedidosyacl_sections WHERE active""")
        self.cursor.execute("""SELECT section_id FROM history.pedidosya_category""")
        selected_sections= list(map(lambda x: x[0],self.cursor.fetchall()))
        connection.close()
        for p_id, bt, ss in selected_partners:
            # Get selections as list
            selections = ast.literal_eval(ss)
            section_in_partner = list(filter(lambda x: x in selected_sections, selections))
            for s_id in section_in_partner:
                #if x['section_id']:
                url = f"https://www.pedidosya.cl/profile/getProductsToSection?sectionId={s_id}&partnerId={p_id}&discount=0&offset=0&businessType={bt}&pharmaDisablePrice=true"
                yield scrapy.Request(url, callback=self.products, meta={'p_id':p_id, 's_id':s_id})
        insert_log('End', 'Pedidos Ya')


    def products(self,r):
        products_listed = r.xpath('//li')
        for p in products_listed:
            item = {
                'product_id': p.xpath('./@data-id').get(),
                'name': p.xpath('./div/span[@class="shelves_product_name productName"]/text()').get(),
                'price': p.xpath('.//div[@class="price"]/span/text()').get(),
                'section_id': r.meta['s_id'],
                'partner_id': r.meta['p_id'],
                }

            self.cursor.execute("""INSERT INTO pedidosyacl_products VALUES (%s, %s, %s, %s, %s)""", (
                    item['product_id'],
                    item['name'],
                    item['price'],
                    item['section_id'],
                    item['partner_id']
            ))
            self.connection.commit()

            yield item


def insert_log(tipo, scraper):
    engine = create_engine('postgresql://jiglesias:jigle2020@34.69.175.136:5432/pimvault'
         , connect_args={'options': '-csearch_path={}'.format('ctrl')})

    create_log = "INSERT INTO ctrl.scrapers_logs VALUES (clock_timestamp(), '"+str(tipo)+": Scraper "+str(scraper)+"');"

    #connection = engine.connect()
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(create_log)
    conn.commit()
    return
        
    #     for x in selected:
    #         #url = f"https://www.pedidosya.cl/profile/getProductsToSection?sectionId={x[0]}&partnerId={x[1]}&discount=0&offset=0&businessType={x[2]}&pharmaDisablePrice=true"
    #         url = f"https://www.pedidosya.cl/profile/getProductModal?id={x[0]}&partnerId={x[2]}"
    #         yield scrapy.Request(url, callback=self.update_product, meta={'section_id':x[0], 'partner_id':x[1]})


    # def update_section(self, r):
    #     pass

    # def update_partner(self,r):
    #     pass

    # def update_product(self,r):
    #     import ipdb; ipdb.set_trace()
    #     item = {
    #         'product_id': r.xpath('//header/@data-productid').get(),
    #         'name': r.xpath('//div[@class="info "]/h2/text()').get(),
    #         'price': r.xpath('//div[@class="price "]/@data-price').get(),
    #         'section_id': r.meta['section_id'],
    #         'partner_id': r.meta['partner_id'],
    #         }

    #     self.cursor.execute("""INSERT INTO pedidosyacl_products VALUES (%s, %s, %s, %s, %s)""", (
    #             item['product_id'],
    #             item['name'],
    #             item['price'],
    #             item['section_id'],
    #             item['partner_id']
    #     ))
    #     self.connection.commit()

    #     yield item
        
            

        

        
