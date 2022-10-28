from glob import glob
import pandas as pd
import requests as rq
import os
import threading
from numpy import linspace
from UrlMaker import UrlMaker
from pdb import set_trace

def url_exists(urlsToCheck,idx):
    for url in urlsToCheck:
        try:
            #print(f'trying {url}')
            response = rq.get(url, allow_redirects=False, timeout=3)
            if response.status_code==200:
                with open('tmp_urls.txt', 'a+') as f:
                    f.write(f'{idx},{url}\n')
        except:
            pass

def threadFunc(rows):
    url_maker = UrlMaker()
    for idx,firm in rows.iterrows():
        possibleUrls = url_maker.make_urls(firm['name'])
        url_exists(possibleUrls, idx)


def main():
    max_threads = 128
    files = glob( os.path.join('..', 'firms','firms*pkl') )
    df = pd.concat([pd.read_pickle(file) for file in files], ignore_index=True) # merge everything into a single dataframe
    df['url'] = pd.Series([[] for _ in range(len(df.index))])
    batches = linspace(0,len(df),max_threads, dtype=int)
    threads = []
    for start,stop in zip(batches[:-1],batches[1:]):
        t = threading.Thread(target=threadFunc, args=(df.iloc[start:stop],))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    with open('tmp_urls.txt') as f:
        for l in f.readlines():
            idx,url = l.strip().split(',')
            df.iloc[int(idx)]['url'].append(url)
    os.remove('tmp_urls.txt')
    df.to_pickle( os.path.join('firms','all.pkl') )
    df.to_excel( os.path.join('firms','all.xlsx') )


if __name__=='__main__':
    main()
