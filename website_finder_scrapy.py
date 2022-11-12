from glob import glob
import pandas as pd
import sys, os, logging, jsonlines, multiprocessing
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from attestabot.spiders.WebsiteFinder import WebsiteFinder
from website_finder.UrlMaker import UrlMaker
from pdb import set_trace


def finalize_and_save_dataframe(pkl_outfile, df):
    df.loc[df.url_exists.isna(), 'url_exists'] = 'FALSE'
    df.loc[df.url_checked_on.isna(), 'url_checked_on'] = today
    df.to_pickle( pkl_outfile )
    df.to_excel( pkl_outfile.replace('.pkl', '.xlsx') )
    del df


def add_urls_to_dataframe_kernel(df):
    url_maker = UrlMaker()
    new_rows  = []
    for url in url_maker.make_urls(df.iloc[0]['name']):
        if url not in df.url.values:
            new_row = df.iloc[0].copy()
            new_row.url = url
            new_row.url_exists = None
            new_row.url_checked_on = None
            new_rows.append(new_row.to_frame().T)
    if len(new_rows)==0:
        return None
    return pd.concat(new_rows)


def add_urls_to_dataframe(df):
    if 'url' not in df.columns:
        for new_column in ('url', 'url_exists', 'url_checked_on'):
            df[new_column] = pd.Series([None for _ in range(len(df.index))])

    with multiprocessing.Pool() as pool:
        all_new_rows = pool.map(add_urls_to_dataframe_kernel, (df.query('name==@name') for name in set(df['name'])))
    df = pd.concat([df.dropna(subset='url'), *all_new_rows])
    df.sort_values(by='name', ignore_index=True, inplace=True)
    logging.info(f'{df.iloc[0].canton}: added rows for urls')
    return df


def main():
    LOGFILE = 'website_finder.log'
    crawler_settings = get_project_settings()
    crawler_settings['DOWNLOAD_TIMEOUT']     = 15
    crawler_settings['FEED_EXPORT_ENCODING'] = 'utf-8'
    crawler_settings['ROBOTSTXT_OBEY']       = False
    crawler_settings['LOG_FILE']             = LOGFILE
    crawler_settings['LOG_ENABLED']          = False
    crawler_settings['LOG_FILE_APPEND']      = False
    crawler_settings['LOG_LEVEL']            = 'INFO'
    crawler_settings['COOKIES_ENABLED']      = False
    crawler_settings['RETRY_ENABLED']        = False
    crawler_settings['FEEDS']                = {
            'existing_urls.jl' : {'format': 'jsonlines'}
            }
    crawler_process = CrawlerProcess(crawler_settings)

    files = glob( os.path.join('firms','firms*pkl') )
    df_dict = {}
    for pkl_file in files:
        df = pd.read_pickle(pkl_file)
        logging.info(f'loading {pkl_file}, with {len(set(df["name"]))} firms')
        df_dict[pkl_file] = add_urls_to_dataframe(df)
        df_dict[pkl_file].to_pickle( pkl_file )
        crawler_process.crawl(WebsiteFinder, pkl_file=pkl_file)
    crawler_process.start()

    logging.info('done crawling, will now write existing urls to df')

    with jsonlines.open('existing_urls.jl') as jl_file:
        for site in jl_file:
            try:
                assert(site['url'] == df_dict[site['pkl_file']].iloc[site['df_index']].url)
                df_dict[site['pkl_file']].at[site['df_index'], 'url_exists'] = 'TRUE'
            except:
                logging.warning(f'index {site["df_index"]} not matching for {site["pkl_file"]}')
                continue

    logging.info('done writing existing urls to df, will now write to disk and finish')
    today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    with multiprocessing.Pool() as pool:
        pool.starmap(finalize_and_save_dataframe, df_dict.items())
    logging.info('all done')


if __name__=='__main__':
    main()
