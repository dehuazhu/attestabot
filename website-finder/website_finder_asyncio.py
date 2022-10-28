from glob import glob
import pandas as pd
import asyncio, aiohttp
import os
from UrlMaker import UrlMaker
from pdb import set_trace

async def url_exists(urls_to_check,idx, session):
    for url in urls_to_check:
        try:
            #print(f'trying {url}')
            async with session.get(url, allow_redirects=False, timeout=3) as response:
                if response.status_code==200:
                    with open('tmp_urls.txt', 'a+') as f:
                        f.write(f'{idx},{url}\n')
        except:
            pass

async def check_all_urls(df):
    url_maker = UrlMaker()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx,firm in df.iterrows():
            possible_urls = url_maker.make_urls(firm['name'])
            task = asyncio.ensure_future(url_exists(possible_urls, idx, session))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

def main():
    files = glob( os.path.join('..', 'firms','firms_AI*pkl') )
    df = pd.concat([pd.read_pickle(file) for file in files], ignore_index=True) # merge everything into a single dataframe
    df['url'] = pd.Series([[] for _ in range(len(df.index))])
    asyncio.run(check_all_urls(df))
    with open('tmp_urls.txt') as f:
        for l in f.readlines():
            idx,url = l.strip().split(',')
            df.iloc[int(idx)]['url'].append(url)
    os.remove('tmp_urls.txt')
    df.to_pickle( os.path.join('..','firms','all.pkl') )
    df.to_excel( os.path.join('..','firms','all.xlsx') )


if __name__=='__main__':
    main()
