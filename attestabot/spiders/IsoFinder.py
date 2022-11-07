from scrapy import Request, Item
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import os
import pandas as pd
from glob import glob
from pdb import set_trace

def process_request(request, response):
    for attr in ('pkl_file', 'index', 'url'):
        request.meta[attr] = response.request.meta.get(attr)
    return request

class IsoFinder(CrawlSpider):
    name = 'isofinder'
    rules = [
            Rule(LinkExtractor(), follow=True, callback='parse_item', process_request=process_request),
            ]

    def start_requests(self):
        yield Request('https://joos-metallbau.ch/')
        #files = glob( os.path.join('firms','firms_BS*pkl') )
        #for pkl_file in files:
        #    df = pd.read_pickle(pkl_file)
        #    for index, url in df.query('url_exists=="TRUE"')['url'].iteritems():
        #        meta = {
        #                'pkl_file' : pkl_file,
        #                'index'    : index,
        #                'url'      : url,
        #                }
        #        yield Request(url, self.parse, meta=meta)

    def parse_item(self, response):
        #with open(f'{response.meta["index"]}.log', 'w') as f:
        #    f.write(f"{response.meta['pkl_file']}, {response.meta['index']}")
        #set_trace()
        if 'ISO' in response.body.decode('utf-8'):
            #with open(f'asda.log', 'w') as f:
            #    f.write(f'found ISO at {response.url}')
            yield {'url_with_iso' : response.url}
