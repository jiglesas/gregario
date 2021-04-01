import scrapy
from scrapy.shell import inspect_response
import psycopg2
from psycopg2 import Error
import re, json


from .pedidosya_base import PedidosyaBaseSpider

class PedidosyaPSSpider(PedidosyaBaseSpider):
    name = 'pedidosyacl_partners-sections'
   
    def start_requests(self):
        self.create_connection()
        # Check for new
        yield scrapy.Request('http://pedidosya.cl/')

    def get_coordenates(self):
        # Get coordenates from DB
        self.cursor.execute("SELECT * from scrapinghub.geolocation_new_pedidosya")
        rs = self.cursor.fetchall()

        # Return all coordenates
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
        partners = r.xpath('//ul[@id="resultList"]/li[@data-url]')
        for p in partners:
            url = p.xpath('./@data-url').get()
            try:
                pd = json.loads(p.xpath('./@data-info').get())
                tags = list(map(lambda x: (x['id'], x['name']), pd['topCategories']))
            except:
                tags = []
            item['local_info']['tags'] = tags
            yield scrapy.Request(url, callback=self.landing_main, meta={'item':item})

        #Next page
        next_p = r.xpath('//li[@class="arrow next"]/a/@href').get()
        if next_p:
            yield scrapy.Request(next_p, callback=self.main_category, meta={'item':item})


    def landing_main(self,r):
        item = r.meta['item'].copy()

        # Local/partner data
        item['local_info'].update ({
            'partner_id': r.xpath('//div[@id="profileHeader"]/@data-id').get(),
            'catalogue_id': r.xpath('//section[@id="menu"]/@data-catid').get(),
            'description': r.xpath('//meta[@property="og:description"]/@content').get(),
            'name': r.xpath('//h1[@itemprop="name"]/text()').get(),
            'address': r.xpath('//*[@itemprop="streetAddress"]/text()').get(),
            'url': r.url, #r.xpath('//meta[@property="og:url"]/@content').get(),
            'sections': [],     
        })

        r.meta['item'] = item
        yield from self.sections(r)

    def sections(self, r):
        item = r.meta['item'].copy()
        data_index = r.meta.get('data_index')
        # Get sections
        section_raw = r.xpath('//section[@id="menu"]//div[@class="sectionTitle"]/h3')
        for x in section_raw:
            # Get or generate position (index)
            site_index = x.xpath('./@data-index').get()
            if site_index:
                data_index = int(site_index)
            elif data_index:
                data_index +=1
            else: 
                data_index = -1

            item['local_info']['sections'].append({
                                                  'name': x.xpath('./@data-name').get(), 
                                                  'index': str(data_index),
                                                  'section_id': x.xpath('./@data-id').get()
                                                  })

        # Deprecated
        # item['local_info']['sections'].extend([{'name': x.xpath('./@data-name').get(), 'index': x.xpath('./@data-index').get(),'section_id': x.xpath('./@data-id').get()} for x in section_raw])

        # Get more sections
        if r.xpath('//button[@class="btn-more-section"]'):
            url = f"https://www.pedidosya.cl/profile/getMoreSectionProducts?catalogueId={item['local_info']['catalogue_id']}&partnerId={item['local_info']['partner_id']}&page=20&discount=0&businessType={item['local_info']['business_type']}&pharmaDisablePrice=true"
            yield scrapy.Request(url, callback=self.sections, meta={'item':item, 'data_index': data_index})
        else:
            yield from self.process(item)


    def process(self, item):
        # Insert partner to DB
        self.cursor.execute("""INSERT INTO pedidosyacl_partners VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", (
            item['local_info']['partner_id'],
            item['local_info']['business_type'],
            item['local_info']['name'],
            item['local_info']['address'],
            item['local_info']['url'],
            item['local_info']['description'],
            str(list(filter(None, map(lambda x: x['section_id'] ,item['local_info']['sections'])))),
            str(item['local_info']['tags']),
        ))
        self.connection.commit()

        for s in item['local_info']['sections']:
            # Insert section to DB 
            self.cursor.execute("""INSERT INTO pedidosyacl_sections VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""", (
                s['section_id'],
                s['name'],
                s['index'],
            ))
            self.connection.commit()

        yield item
        

        
