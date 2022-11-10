from scrapy import Request, Item
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor, IGNORED_EXTENSIONS
import os
import pandas as pd
from glob import glob
from urllib.parse import urlparse
from pdb import set_trace

MAX_HITS_PER_HOST = 10

KEYWORDS_URL = (
        'ISO',
        'Zertifikat',
        )

KEYWORDS_CERTIFICATE = (
        'iso',
        'zertifikat',
        )

ALLOWED_EXTENSIONS = (
        'pdf',
        'png',
        )

def process_links(links):
    set_trace()
    yield link


class IsoFinder(CrawlSpider):
    name = 'isofinder'

    custom_settings = {
            'LOG_LEVEL'              : 'INFO',
            'DEPTH_LIMIT'            : 2,
            #'DEPTH_PRIORITY'         : 1,
            #'SCHEDULER_DISK_QUEUE'   : 'scrapy.squeues.PickleFifoDiskQueue',
            #'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
            }

    ignored_extensions = IGNORED_EXTENSIONS
    for ext in ALLOWED_EXTENSIONS:
        ignored_extensions.remove(ext)
    rules = [
            Rule(LinkExtractor(deny_extensions=ignored_extensions), follow=True, callback='parse_item', process_request='process_request'),
            #Rule(LinkExtractor(deny_extensions=ignored_extensions), follow=True, callback='parse_item', process_links=process_links, process_request=process_request),
            ]


    def process_request(self, request, response):
        hostname = urlparse(response.url).hostname
        if self.host_counter[hostname]>MAX_HITS_PER_HOST:
            return None
        if (not hostname in request.url) and (not request.url.endswith(ALLOWED_EXTENSIONS)):
            # exclude links on other domains (hostnames), unless they are a file with an allowed extensions (files are sometimes hosted on other domains)
            return None
        for attr in ('pkl_file', 'index', 'url'):
            request.meta[attr] = response.request.meta.get(attr)
        return request

    def __init__(self, *args, **kwargs):
        super(IsoFinder, self).__init__(*args, **kwargs)
        #self.pkl_file = kwargs.get('pkl_file')
        self.host_counter = {}
        #self.yielded_links = []

    def start_requests(self):
        for url in (
                'https://wettstein-produktion.ch/', # ok
                'https://juice.world/', #finds way too many links
                #'http://www.designwerk.com/', #error 510 ??
                'https://www.alz-maschinen.ch/', #ok, needs DEPTH_LIMIT=2
                'https://aa-praezisionsmechanik.ch/', #no: contains only a png with no keyword in the file name and no reference to keywords in the text
                'https://www.almutechag.ch/', #ok
                'https://www.blue-o.ch/', #ok
                'https://www.hjakober.ch/',
                ):
            self.host_counter[urlparse(url).hostname] = 0
            yield Request(url)
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
        hostname = urlparse(response.url).hostname
        if self.host_counter[hostname]<=MAX_HITS_PER_HOST:
            if response.url.lower().endswith(ALLOWED_EXTENSIONS) and any(kw in response.url.lower() for kw in KEYWORDS_CERTIFICATE):
                self.host_counter[hostname] += 1
                yield {'link_to_file' : response.url}
            #elif any(kw in response.body.decode('utf-8') for kw in KEYWORDS_URL):
            elif hasattr(response, 'text') and any([response.xpath(f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{kw}')]/text()").getall() for kw in KEYWORDS_CERTIFICATE]):
                self.host_counter[hostname] += 1
                yield {'link_to_page' : response.url}
            elif hasattr(response, 'text') and any([link for link in response.css('img').xpath('@src').getall() if any(kw in link.lower() for kw in KEYWORDS_CERTIFICATE)]):
                self.host_counter[hostname] += 1
                yield {'link_with_image' : response.url}
                #for link_to_img in [link for link in response.css('img').xpath('@src').getall() if any(kw in link.lower() for kw in KEYWORDS_CERTIFICATE)]:
                #    #for embedded images e.g. https://juice.world/
                #    if not link_to_img.startswith('http'):
                #        link_to_img = response.urljoin(link_to_img)
                #    if not link_to_img in self.yielded_links:
                #        self.yielded_links += [link_to_img]
                #        yield {'link_to_file' : link_to_img}
