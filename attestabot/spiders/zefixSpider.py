import scrapy
import os, json, jsonlines
import pandas as pd
from pdb import set_trace

OUTDIR = 'firms_zefix'

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
    name = 'zefix'

    def start_requests(self):
        if not os.path.exists(OUTDIR):
            os.makedirs(OUTDIR)
        #yield scrapy.Request.from_curl( get_curl(280, 0) )
        for cantonID in cantonIDs.values():
            yield scrapy.Request.from_curl( get_curl(cantonID, 0) )

    def parse(self, response):
        cantonID,   = json.loads(response.request.body.decode('utf-8'))['registryOffices']
        cantonName, = [canton for canton,ID in cantonIDs.items() if cantonID==ID]
        outfileName = f'firms_{cantonName}'
        response_json = response.json()
        #for firm in response_json['list']:
        #    yield firm
        #with open(f'firms_{cantonName}.json','w') as outfile:
        #    outfile.write(json.dumps(response_json['list'], indent=2, ensure_ascii=False))
        with jsonlines.open(os.path.join(OUTDIR,f'{outfileName}.jl'), mode='a') as outfile:
            outfile.write_all(response_json['list'])
        if response_json['hasMoreResults']:
            offset = response_json['maxOffset']
            yield scrapy.Request.from_curl( get_curl(cantonID, offset) )
        else:
            with jsonlines.open(os.path.join(OUTDIR,f'{outfileName}.jl')) as jlFile:
                df = pd.DataFrame.from_dict([line for line in jlFile])
            df.insert(8, 'cantonId', cantonID)
            df.insert(9, 'canton', cantonName)
            df.to_pickle(os.path.join(OUTDIR,f'{outfileName}.pkl'))
            os.remove(os.path.join(OUTDIR,f'{outfileName}.jl'))
