from scrapy import Spider, Request, Item
import pandas as pd
from glob import glob
import os
from pdb import set_trace

class WebsiteFinder(Spider):
    name = 'website-finder'

    def __init__(self, *args, **kwargs):
        super(WebsiteFinder, self).__init__(*args, **kwargs)
        self.pkl_file = kwargs.get('pkl_file')

    def start_requests(self):
        pkl_file = self.pkl_file
        df = pd.read_pickle(pkl_file)
        rows_to_check = (df.url_exists.isna()) | (df.url_exists=='FALSE')
        for idx,firm in df[rows_to_check].iterrows():
            meta = {
                    'pkl_file' : pkl_file,
                    'df_index' : idx,
                    'url'      : firm['url']
                    }
            yield Request(firm['url'], self.parse, meta=meta)

    def parse(self, response):
        if response.status==200:
            yield {
                    'pkl_file'   : response.meta['pkl_file'],
                    'df_index'   : response.meta['df_index'],
                    'url'        : response.meta['url'],
                    }
