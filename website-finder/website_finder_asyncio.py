from glob import glob
import pandas as pd
import asyncio, aiohttp
import os
from UrlMaker import UrlMaker
from pdb import set_trace

async def url_exists(urls_to_check,idx, session):
    valid_urls = []
    for url in urls_to_check:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=5)) as response:
                if response.status==200:
                    valid_urls += [(idx,url)]
        except:
            pass
    if len(valid_urls)>0:
        return valid_urls

async def check_all_urls(df):
    url_maker = UrlMaker()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx,firm in df.iterrows():
            possible_urls = url_maker.make_urls(firm['name'])
            task = asyncio.ensure_future(url_exists(possible_urls, idx, session))
            tasks.append(task)
        return await asyncio.gather(*tasks, return_exceptions=True)

def main():
    files = glob( os.path.join('..', 'firms','firms*pkl') )
    df = pd.concat([pd.read_pickle(file) for file in files], ignore_index=True) # merge everything into a single dataframe
    df['url'] = pd.Series([[] for _ in range(len(df.index))])
    valid_urls = asyncio.run(check_all_urls(df))
    for result in valid_urls:
        if result is not None:
            for idx,url in result:
                df.iloc[int(idx)]['url'].append(url)
    df.to_pickle( os.path.join('..','firms','all.pkl') )
    df.to_excel( os.path.join('..','firms','all.xlsx') )


if __name__=='__main__':
    main()
