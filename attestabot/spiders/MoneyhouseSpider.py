import scrapy, logging
import pandas as pd
from datetime import datetime
from pdb import set_trace

class MoneyhouseSpider(scrapy.Spider):
    name = 'moneyhouse_spider'

    def __init__(self, parquet_file=None, starting_line=0, max_requests_per_file=None, *args, **kwargs):
        super(MoneyhouseSpider, self).__init__(*args, **kwargs)
        self.parquet_file          = parquet_file
        self.starting_line         = starting_line
        self.max_requests_per_file = max_requests_per_file
        logging.info(f'Initialized Spider for {parquet_file}')

    def start_requests(self):
        url  = 'https://service.moneyhouse.ch/api/login'
        with open('moneyhouse.login') as f:
            lines = f.readlines()
            user  = lines[0].strip()
            pwd   = lines[1].strip()
        body = f'{{"email":"{user}","password":"{pwd}"}}'

        logging.debug(f'Logging in for {self.parquet_file}')
        yield scrapy.Request(
                url         = url,
                method      = 'POST',
                dont_filter = True,
                body        = body,
                callback    = self.make_requests
                )


    def make_requests(self, response):
        #set_cookie = response.headers[b'Set-Cookie'].decode().split('; ')
        #user,      = [cookie.replace('user=','') for cookie in set_cookie if cookie.startswith('user=')]
        #token      = response.json()['token']
        #cookies    = {
        #        "token": token,
        #        "user" : user,
        #        }

        search_url = lambda query: f'https://www.moneyhouse.ch/de/search?q={query}'
        df = pd.read_parquet(self.parquet_file)

        #start = self.starting_line
        #stop  = start + self.max_requests_per_file
        #for idx, firm in df[start:stop].iterrows():
        for idx, firm in df.iterrows():
            search_str = firm.chid
            url  = search_url(search_str)
            meta = {
                    'parquet_file' : self.parquet_file,
                    'df_index'     : idx,
                    }

            logging.debug(f'Submitting request to {url} for {self.parquet_file}')
            yield scrapy.Request(
                    url         = url,
                    method      = 'GET',
                    dont_filter = True,
                    #cookies     = cookies,
                    meta        = meta,
                    callback    = self.parse
                    )

    def parse(self, response):
        today    = datetime.strftime(datetime.now(), '%Y-%m-%d')
        address  = None
        tel      = None
        mail     = None
        homepage = None
        other    = []

        address_raw, = [cell for cell in response.css('div.section div.card div.section div.l-grid div.l-grid-cell') if cell.css('h4.key::text').get()=='Adresse']
        if address_raw:
            address = ', '.join(address_raw.css('p::text').getall()).strip()

        connection_info = response.css('div.connections-row a::attr(href)').getall()
        for connection in connection_info:
            if connection.startswith('tel:'):
                tel = connection.replace('tel:','')
            elif connection.startswith(('http','www')):
                homepage = connection
            elif connection.startswith('mailto:'):
                mail = connection.replace('mailto:','')
            else:
                other += [connection]

        yield {
                'parquet_file' : response.meta['parquet_file'],
                'df_index'     : response.meta['df_index'],
                'checked_on'   : today,
                'address'      : address,
                'tel'          : tel,
                'mail'         : mail,
                'homepage'     : homepage,
                'other'        : other,
                }
