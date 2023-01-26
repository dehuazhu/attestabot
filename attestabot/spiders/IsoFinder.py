from scrapy import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import os, logging
from datetime import datetime
import pandas as pd
from urllib.parse import urlparse
from pdb import set_trace

#MAX_HITS_PER_HOST_PAGE_WITH_KW  = 10
#MAX_HITS_PER_HOST_PAGE_WITH_IMG = 5
MAX_HITS_PER_HOST_TOTAL         = 20
MAX_REQUESTS_PER_HOST           = 700

KEYWORDS_CERTIFICATE_NO_ISO = (
        'zertifikat',
        'zertifizierung',
        'certifica',
        )

KEYWORDS_CERTIFICATE = KEYWORDS_CERTIFICATE_NO_ISO + ('iso',)

ALLOWED_EXTENSIONS = (
        'pdf',
        'png',
        'jpg',
        'jpeg',
        )


class IsoFinder(CrawlSpider):
    name = 'iso-finder'

    custom_settings = {
            'CONCURRENT_REQUESTS'    : 200,
            'DOWNLOAD_DELAY'         : .25,
            #'ROBOTSTXT_OBEY'         : False,
            'DOWNLOAD_TIMEOUT'       : 15,
            'DEPTH_LIMIT'            : 4,
            'DEPTH_PRIORITY'         : 1,
            'SCHEDULER_DISK_QUEUE'   : 'scrapy.squeues.PickleFifoDiskQueue',
            'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
            'COOKIES_ENABLED'        : False,
            'RETRY_ENABLED'          : False,
            }

    rules = [
            Rule(LinkExtractor(), follow=True, callback='parse_item', process_request='process_request'),
            ]


    #placeholder, may be useful in the future
    #def process_links(self, links):
    #    for link in links:
    #        yield link


    def process_request(self, request, response):
        hostname = urlparse(response.request.meta.get('company_homepage')).hostname
        hostname_current = urlparse(response.url).hostname #can be different from hostname because of valid redirects
        if (hostname_current != urlparse(request.url).hostname):
            logging.debug(f'Dropping request to a host different than {hostname_current}: {request.url}.')
            return None
        if sum(self.hit_counter[hostname].values())>MAX_HITS_PER_HOST_TOTAL:
            logging.debug(f'Max hits per host {hostname} exceeded (reached {sum(self.hit_counter[hostname].values())}), dropping new requests.')
            return None
        if self.request_counter[hostname]>=MAX_REQUESTS_PER_HOST:
            logging.debug(f'Max requests ({self.request_counter[hostname]}) reached for {hostname}, dropping new requests.')
            return None
        for attr in ('df_index', 'name', 'company_homepage'):
            request.meta[attr] = response.request.meta.get(attr)
        self.request_counter[hostname] += 1
        return request


    def __init__(self, *args, **kwargs):
        super(IsoFinder, self).__init__(*args, **kwargs)
        self.parquet_file    = kwargs.get('parquet_file')
        self.request_counter = {}
        self.hit_counter     = {}

    def start_requests(self):
        df = pd.read_parquet(self.parquet_file)
        #for idx, (name,url) in df.query('url_exists=="TRUE"')[['name','url']].iterrows():
        for idx, (name,url) in df[df.homepage.notna()][['name','homepage']].iterrows():
            hostname = urlparse(url).hostname
            self.request_counter[hostname] = 0
            self.hit_counter[hostname]     = {
                    'page_with_file' : 0,
                    'page_with_kw'   : 0,
                    'page_with_logo' : 0,
                    }
            meta = {
                    'df_index'         : idx,
                    #'name'      : hostname, #FIXME
                    'name'             : name,
                    'company_homepage' : url,
                    }
            logging.info(f'Submitting {url=} with {hostname=}')
            yield Request(url, meta=meta)

    def parse_item(self, response):
        today = datetime.strftime(datetime.now(), '%Y-%m-%d')
        if hasattr(response, 'text'):
            item = {
                    'parquet_file'          : self.parquet_file,
                    'df_index'              : response.meta['df_index'],
                    'iso_finder_checked_on' : today,
                    #'name'                  : response.meta['name'],
                    #'homepage'              : response.meta['company_homepage'],
                    'suburl_with_iso_info'  : response.url,
                    'suburl_has_iso_file'   : 'FALSE',
                    'suburl_is_iso_file'    : 'FALSE',
                    'suburl_has_keyword'    : 'FALSE',
                    'suburl_has_logo'       : 'FALSE',
                    }
            #hostname = urlparse(response.url).hostname
            hostname = urlparse(response.meta['company_homepage']).hostname
            if sum(self.hit_counter[hostname].values())<MAX_HITS_PER_HOST_TOTAL:
                if any(files := [link for link in response.css('*::attr(href)').getall() if (urlparse(link).path.lower().endswith(ALLOWED_EXTENSIONS) and any(kw in link.lower() for kw in KEYWORDS_CERTIFICATE))]):
                    self.hit_counter[hostname]['page_with_file'] += 1
                    item['suburl_has_iso_file'] = 'TRUE'
                if any(text for text in response.css('*::text').getall() if (
                        ' ISO ' in text
                        or text.startswith('ISO ')
                        or text.endswith((' ISO', 'ISO.'))
                        or any(kw in text.lower() for kw in KEYWORDS_CERTIFICATE_NO_ISO)
                        )):
                    self.hit_counter[hostname]['page_with_kw'] += 1
                    #if self.hit_counter[hostname]['page_with_kw']<=MAX_HITS_PER_HOST_PAGE_WITH_KW:
                    logging.debug(f'found page with keyword at {response.url}')
                    item['suburl_has_keyword'] = 'TRUE'
                if any(link for link in response.css('img').xpath('@src').getall() if any(kw in link.lower() for kw in KEYWORDS_CERTIFICATE)):
                    self.hit_counter[hostname]['page_with_logo'] += 1
                    #if self.hit_counter[hostname]['page_with_logo']<=MAX_HITS_PER_HOST_PAGE_WITH_IMG:
                    logging.debug(f'found page with logo at {response.url}')
                    item['suburl_has_logo'] = 'TRUE'
                if 'TRUE' in (item['suburl_has_iso_file'], item['suburl_has_keyword'], item['suburl_has_logo']):
                    yield item
                if files:
                    item['suburl_is_iso_file']  = 'TRUE'
                    item['suburl_has_iso_file'] = 'FALSE'
                    item['suburl_has_keyword']  = 'FALSE'
                    item['suburl_has_logo']     = 'FALSE'
                    for link_to_file in files:
                        item['suburl_with_iso_info'] = response.urljoin(link_to_file)
                        yield item

    def parse_start_url(self, response):
        return self.parse_item(response)
