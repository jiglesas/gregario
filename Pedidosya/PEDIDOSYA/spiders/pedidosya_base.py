import scrapy
from scrapy.shell import inspect_response
import psycopg2
from psycopg2 import Error

class PedidosyaBaseSpider(scrapy.Spider):
    name = 'pedidosya_base'
    allowed_domains = ['pedidosya.cl']
    #start_urls = ['http://pedidosya.cl/']
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

    def create_connection(self):
        try:
            self.connection = psycopg2.connect(user="jiglesias",
                                          password="jigle2020",
                                          host="34.69.175.136",
                                          port="5432",
                                          database="pimvault",
                                          options="-c search_path=dbo,scrapinghub")
            self.connection.autocommit = True

            self.cursor = self.connection.cursor()

        except (Exception, Error) as error:
            log.error(f"Error while connecting to PostgreSQL: {error}")
        # finally:
        #     if (connection):
        #         cursor.close()
        #         connection.close()
        #         print("PostgreSQL connection is closed")





        

        
