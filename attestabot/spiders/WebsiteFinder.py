from scrapy import Spider, Request, Item
import pandas as pd
from glob import glob
import os
from pdb import set_trace

class WebsiteFinder(Spider):
    name = 'website-finder'

    def __init__(self, *args, **kwargs):
        super(WebsiteFinder, self).__init__(*args, **kwargs)
        self.parquet_file = kwargs.get('parquet_file')

    def start_requests(self):
        parquet_file = self.parquet_file
        df = pd.read_parquet(parquet_file)
        rows_to_check = (df.url_exists.isna()) | (df.url_exists=='FALSE')
        for idx,firm in df[rows_to_check].iterrows():
            meta = {
                    'parquet_file' : parquet_file,
                    'df_index'     : idx,
                    'url'          : firm['url']
                    }
            yield Request(firm['url'], self.parse, meta=meta)

    def parse(self, response):
        if response.status==200:
            yield {
                    'parquet_file'   : response.meta['parquet_file'],
                    'df_index'       : response.meta['df_index'],
                    'url'            : response.meta['url'],
                    }
