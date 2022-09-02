from glob import glob
import pandas as pd
import requests as rq
import os
import threading
from pdb import set_trace

def make_urls(firmName):
    urlJoins = ('', '-')
    TLDs     = ('ch','com')

    possibleUrls = []
    firmName = firmName.lower()
    nameList = firmName.split(' ')
    if nameList[-1] in ('sagl','sa','ag','gmbh','sàrl'):
        nameList = nameList[:-1]
    for join in urlJoins:
        baseUrl = join.join(nameList)
        for tld in TLDs:
            url = f'http://{baseUrl}.{tld}'
            possibleUrls.append(url)
        if len(nameList)==1: break
    return possibleUrls

def url_exists(urlsToCheck,validUrls):
    for url in urlsToCheck:
        try:
            #print(f'trying {url}')
            response = rq.get(url, allow_redirects=False)
            if response.status_code==200:
                validUrls.append(url)
        except:
            pass


def main():
    max_threads = 100
    files = glob( os.path.join('firms','firms*pkl') )
    for path in files:
        if not 'AI' in path: continue
        print(f'starting with {path}')
        df = pd.read_pickle(path)
        df['url'] = pd.Series([[] for _ in range(len(df.index))])
        threads = []
        for idx,firm in df.iterrows():
            #if idx>10: break
            possibleUrls = make_urls(firm['name'])
            t = threading.Thread(target=url_exists, args=(possibleUrls,firm['url']))
            threads.append(t)
        #if (idx+1)%max_threads==0 or (idx+1)==len(df):
        for t in threads:
            t.start()
                #t.join(10)
            #threads = []
        set_trace()
            

if __name__=='__main__':
    main()
