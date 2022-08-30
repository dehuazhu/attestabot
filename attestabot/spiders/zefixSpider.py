import scrapy
import json
from pdb import set_trace

cantonIDs = {
        'AG' : 400,
        'AI' : 310,
        'AR' : 300,
        'BE' : 36,
        'BL' : 280,
        'BS' : 270,
        'FR' : 217,
        'GE' : 660,
        'GL' : 160,
        'GR' : 350,
        'JU' : 670,
        'LU' : 100,
        'NE' : 645,
        'NW' : 150,
        'OW' : 140,
        'SG' : 320,
        'SH' : 290,
        'SO' : 241,
        'SZ' : 130,
        'TG' : 440,
        'TI' : 501,
        'UR' : 120,
        'VD' : 550,
        'VS_Ober'    : 600,
        'VS_Bas'     : 621,
        'VS_Central' : 626,
        'ZG' : 170,
        'ZH' : 20
        }

def get_curl(curl_cantonID, curl_offset):
    return f'curl \'https://www.zefix.ch/ZefixREST/api/v1/firm/search.json\' -X POST -H \'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0\' -H \'Accept: application/json, text/plain, */*\' -H \'Accept-Language: en-US,en;q=0.7,de-CH;q=0.3\' -H \'Accept-Encoding: gzip, deflate, br\' -H \'Content-Type: application/json\' -H \'Origin: https://www.zefix.ch\' -H \'DNT: 1\' -H \'Connection: keep-alive\' -H \'Referer: https://www.zefix.ch/de/search/entity/list?registryOffice=20&searchTypeExact=true\' -H \'Sec-Fetch-Dest: empty\' -H \'Sec-Fetch-Mode: cors\' -H \'Sec-Fetch-Site: same-origin\' --data-raw \'{{"languageKey":"de","maxEntries":5000,"offset":{curl_offset},"searchType":"exact","registryOffices":[{curl_cantonID}]}}\''

class ZefixSpider(scrapy.Spider):
    name = "zefix"

    def start_requests(self):
        #urls = [
        #        'https://www.zefix.ch/de/search/entity/list?registryOffice=310&searchTypeExact=true',
        #]
        #for url in urls:
        #    yield scrapy.Request(url=url, callback=self.parse)
        #yield scrapy.Request.from_curl( get_curl(20, 0) )
        for cantonID in cantonIDs.values():
            yield scrapy.Request.from_curl( get_curl(cantonID, 0) )

    def parse(self, response):
        cantonID,   = json.loads(response.request.body.decode('utf-8'))['registryOffices']
        cantonName, = [canton for canton,ID in cantonIDs.items() if cantonID==ID]
        #for firm in response.json()['list']:
        #    yield firm
        with open(f'firms_{cantonName}.json','w') as outfile:
            outfile.write(json.dumps(response.json()['list'], indent=2, ensure_ascii=False))
        #page = 'test' #response.url.split("/")[-2]
        #filename = f'quotes-{page}.html'
        #with open(filename, 'wb') as f:
        #    f.write(response.body)
        #self.log(f'Saved file {filename}')

        #for quote in response.css('div.quote'):
        #    yield {
        #            'text'   : quote.css('span.text::text').get(),
        #            'author' : quote.css('small.author::text').get(),
        #            'tags'   : quote.css('div.tags a.tag::text').getall(),
        #            }
