import scrapy
from scrapy.shell import inspect_response
import psycopg2
from psycopg2 import Error

class PedidosyaSpider(scrapy.Spider):
    name = 'pedidosyacl_1'
    allowed_domains = ['pedidosya.cl']
    start_urls = ['http://pedidosya.cl/']
    custom_settings = {
        #'CONCURRENT_REQUESTS': 1,
        #'DOWNLOAD_DELAY': 1,
        #'COOKIES_ENABLED': False,
        #'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
        'DEFAULT_REQUEST_HEADERS': { 
                              "Host": "www.pedidosya.cl",
                              "User-Agent": "Mozilla/5.0 (X11 Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0",
                              "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                              "Accept-Language": "en-US,en;q=0.5",
                              "Accept-Encoding": "gzip, deflate, br",
                              "Connection": "keep-alive",
                              "Referer": "https://www.pedidosya.cl/restaurantes/santiago",
                              "Upgrade-Insecure-Requests": "1",
                              "Pragma": "no-cache",
                              "Cache-Control": "no-cache",
                              "TE": "Trailers",
                              }
        }


    def i(self, response):
        ''' Inspect page method'''
        if input('Inspect?') == 'y':
            inspect_response(response, self)

    def get_coordenates(self):
        # Get coordenates from DB
        try:
            connection = psycopg2.connect(user="jiglesias",
                                          password="jigle2020",
                                          host="34.69.175.136",
                                          port="5432",
                                          database="pimvault")

            cursor = connection.cursor()
            cursor.execute("SELECT * from scrapinghub.geolocation WHERE activate = true")
            rs = cursor.fetchall()

        except (Exception, Error) as error:
            log.error(f"Error while connecting to PostgreSQL: {error}")
        finally:
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

        return list(map(lambda x: (x[1],x[2],x[3]),rs))


    def parse(self, r):
        # Iterate over main categories and coordenates
        for bt in ['GROCERIES', 'RESTAURANT']:
            for cs in self.get_coordenates():
                item = {'local_info': {'business_type' : bt}}
                url = f"https://www.pedidosya.cl/restaurantes/{cs[0]}?lng={cs[1]}&lat={cs[2]}&bt={item['local_info']['business_type']}"
                yield scrapy.Request(url, callback=self.main_category, meta={'item':item})

    def main_category(self,r):
        item = r.meta['item'].copy()
        rs = r.xpath('//ul[@id="resultList"]/li/@data-url').getall()
        for url in rs:
            yield scrapy.Request(url, callback=self.landing_main, meta={'item':item})

        #Next page
        next_p = r.xpath('//li[@class="arrow next"]/a/@href').get()
        if next_p:
            yield scrapy.Request(next_p, callback=self.parse, meta={'item':item})
            

    def landing_main(self,r):
        item = r.meta['item'].copy()

        # Bulk meta gathering
        list_filter = ['og:','twitter:', 'fb:', 'al:']
        meta_tags = r.xpath('//meta')
        for t in [{x.xpath('./@name|./@property|./@itemprop').get():x.xpath('./@content').get()} for x in meta_tags]:
            for lf in list_filter:
                for tk, tv in t.items():
                    if tk and lf in tk:
                        item['local_info'].update(t)


        pmc =  r.xpath('//div[@id="paymentMethods"]/div/label')
        schedule = r.xpath('//section[@id="restaurantSchedule"]/ul/li')

        # Local/partner data
        item['local_info'].update ({
            'partner_id': r.xpath('//div[@id="profileHeader"]/@data-id').get(),
            'catalogue_id': r.xpath('//section[@id="menu"]/@data-catid').get(),

            'name': r.xpath('//h1[@itemprop="name"]/text()').get(),
            'local_info': r.xpath('//input[@id="restaurantInfo"]/@value').get(),
            'distance': r.xpath('//div[@id="profileDetails"]/span[@class="distance"]/@title').get(),
            'delivery_time': r.xpath('//div[@id="profileDetails"]/span[@class="deliveryTime"]/@data-time').get(),
            'shipping_amount' : r.xpath('//div[@id="profileDetails"]/span[@class="shippingAmount"]/@title').get(),
            'delivery_amount' : r.xpath('//div[@id="profileDetails"]/span[@class="deliveryAmount"]/@title').get(),
            'paymens_method' : list(map(lambda x: {x.xpath('./text()').get(): x.xpath('./following-sibling::ul[1]/li/span/text()').getall()}, pmc)), 
            
            'hours': list(map(lambda x: {x.xpath('.//div[@class="datecol"]/text()').get():(x.xpath('.//div[@class="hourscol"]/span[@itemprop="opens"]/text()').get(),x.xpath('.//div[@class="hourscol"]/span[@itemprop="closes"]/text()').get())}, schedule)),
            'address': r.xpath('//*[@itemprop="streetAddress"]/text()').get(),

            'sections': [],     
        })

        # Get sections
        section_raw = r.xpath('//section[@id="menu"]//div[@class="sectionTitle"]/h3')
        item['local_info']['sections'].extend([{'section': x.xpath('./@data-name').get(), 'id': x.xpath('./@data-id').get()} for x in section_raw])

        # Get more sections
        if r.xpath('//button[@class="btn-more-section"]'):
            url = f"https://www.pedidosya.cl/profile/getMoreSectionProducts?catalogueId={item['local_info']['catalogue_id']}&partnerId={item['local_info']['partner_id']}&page=20&discount=0&businessType={item['local_info']['business_type']}&pharmaDisablePrice=true"
            yield scrapy.Request(url, callback=self.more_sections, meta={'item':item})
        else:
            for x in item['local_info']['sections']:
                url = f"https://www.pedidosya.cl/profile/getProductsToSection?sectionId={x['id']}&partnerId={item['local_info']['partner_id']}&discount=0&offset=0&businessType={item['local_info']['business_type']}&pharmaDisablePrice=true"
                yield scrapy.Request(url, callback=self.full_list, meta={'item':item, 'section_id':x['id']})


    def more_sections(self,r):
        item = r.meta['item'].copy()

        # Get section
        section_raw = r.xpath('//section[@id="menu"]//div[@class="sectionTitle"]/h3')
        item['local_info']['sections'].extend([{'section': x.xpath('./@data-name').get(), 'id': x.xpath('./@data-id').get()} for x in section_raw])
        
        # Get more section
        if r.xpath('//button[@class="btn-more-section"]'):
            url = f"https://www.pedidosya.cl/profile/getMoreSectionProducts?catalogueId={item['local_info']['catalogue_id']}&partnerId={item['local_info']['partner_id']}&page=20&discount=0&businessType={item['local_info']['business_type']}&pharmaDisablePrice=true"
            yield scrapy.Request(url, callback=self.more_sections, meta={'item':item})

        else:
            #
            # Save section DB????
            #

            for x in item['local_info']['sections']:
                url = f"https://www.pedidosya.cl/profile/getProductsToSection?sectionId={x['id']}&partnerId={item['local_info']['partner_id']}&discount=0&offset=0&businessType={item['local_info']['business_type']}&pharmaDisablePrice=true"
                yield scrapy.Request(url, callback=self.full_list, meta={'item':item, 'section_id':x['id']})

    def full_list(self,r):
        item = r.meta['item'].copy()
        section_id = r.meta['section_id']

        # Get products
        products_listed = r.xpath('//li')
        #products = []
        for p in products_listed:
            item.update({
                    'product_id': p.xpath('./@data-id').get(),
                    'image': p.xpath('./div/img/@src').get().split('?quality')[0],  
                    'name': p.xpath('./div/span[@class="shelves_product_name productName"]/text()').get(),
                    'price': p.xpath('.//div[@class="price"]/span/text()').get(),
                    'partner_id': item['local_info']['partner_id'],
                    'catalogue_id': item['local_info']['catalogue_id'],
                    'section_id': section_id,
                    })
            yield item

        

        
