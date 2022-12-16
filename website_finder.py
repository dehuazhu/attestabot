from glob import glob
import pandas as pd
import sys, os, logging, json, jsonlines, multiprocessing
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from attestabot.spiders.WebsiteFinder import WebsiteFinder
from UrlMaker import UrlMaker
from pdb import set_trace

OUTFILE_RAW   = 'website_finder_raw.jl'
#INPUT_FOLDER  = 'firms_zefix'
INPUT_FOLDER  = 'firms_website_finder'
OUTPUT_FOLDER = 'firms_website_finder'
LOGFILE       = 'website_finder.log'

def finalize_and_save_dataframe(parquet_infile, df):
    today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    df.loc[df.url_exists.isna(), 'url_exists'] = 'FALSE'
    df.loc[df.url_checked_on.isna(), 'url_checked_on'] = today

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    outfile = os.path.basename(parquet_infile)
    outfile = os.path.join(OUTPUT_FOLDER, outfile)
    df.to_parquet( outfile )
    #df.to_excel( outfile.replace('.parquet', '.xlsx') )
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

    #with multiprocessing.Pool() as pool:
    #    all_new_rows = pool.map(add_urls_to_dataframe_kernel, (df.query('name==@name') for name in set(df['name'])))
    all_new_rows = [add_urls_to_dataframe_kernel(df) for df in (df.query('name==@name') for name in set(df['name']))]
    df = pd.concat([df.dropna(subset='url'), *all_new_rows])
    df.sort_values(by='name', ignore_index=True, inplace=True)
    logging.info(f'{df.iloc[0].canton}: added rows for urls')
    return df

def crawler_func(files):
    suffix = 1 if 'firms_FR.parquet' in files else 2
    outfile = OUTFILE_RAW.replace('.jl',f'_{suffix}.jl')
    crawler_settings = get_project_settings()
    crawler_settings['DOWNLOAD_TIMEOUT']     = 30
    crawler_settings['FEED_EXPORT_ENCODING'] = 'utf-8'
    #crawler_settings['ROBOTSTXT_OBEY']       = False
    crawler_settings['LOG_FILE']             = LOGFILE.replace('.log',f'_{suffix}.log')
    crawler_settings['LOG_ENABLED']          = False
    crawler_settings['LOG_FILE_APPEND']      = False
    #crawler_settings['LOG_LEVEL']            = 'INFO'
    crawler_settings['COOKIES_ENABLED']      = False
    #crawler_settings['RETRY_ENABLED']        = False
    crawler_settings['FEEDS']                = {
            outfile : {'format': 'jsonlines'}
            }
    crawler_process = CrawlerProcess(crawler_settings)

    df_dict = {}
    for parquet_file in files:
        parquet_file = os.path.join(INPUT_FOLDER, parquet_file)
        df = pd.read_parquet(parquet_file)
        df_dict[parquet_file] = df
        logging.info(f'loading {parquet_file}, with {len(set(df["name"]))} firms')
        crawler_process.crawl(WebsiteFinder, parquet_file=parquet_file)
    crawler_process.start()

    logging.info(f'done crawling part {suffix}, will now write existing urls to df')

    with open(outfile) as jl_file:
        for line in jl_file.readlines():
            try:
                site = json.loads(line)
            except:
                continue
            if site['url'] == df_dict[site['parquet_file']].iloc[site['df_index']].url:
                df_dict[site['parquet_file']].at[site['df_index'], 'url_exists'] = 'TRUE'
            else:
                logging.warning(f'index {site["df_index"]} not matching for {site["parquet_file"]}')

    logging.info('done writing existing urls to df, will now write to disk and finish')
    for parquet_infile, df in df_dict.items():
        finalize_and_save_dataframe(parquet_infile, df)
    #with multiprocessing.Pool() as pool:
    #    pool.starmap(finalize_and_save_dataframe, df_dict.items())


def main():
    #files = glob( os.path.join(INPUT_FOLDER,'firms*parquet') )
    #for parquet_file in files:
    #    df = pd.read_parquet(parquet_file)
    #    logging.info(f'loading {parquet_file}, with {len(set(df["name"]))} firms')
    #    df = add_urls_to_dataframe(df)
    #    df.to_parquet( parquet_file )
    #    del df
    files_1 = [
            'firms_FR.parquet',
            'firms_NW.parquet',
            'firms_GE.parquet',
            'firms_AR.parquet',
            'firms_TI.parquet',
            'firms_VS_Central.parquet',
            'firms_VD.parquet',
            'firms_BE.parquet',
            'firms_TG.parquet',
            'firms_BL.parquet',
            'firms_UR.parquet',
            'firms_GR.parquet',
            'firms_GL.parquet',
            'firms_SG.parquet'
            ]
    files_2 = [
            'firms_BS.parquet',
            'firms_OW.parquet',
            'firms_ZH.parquet',
            'firms_AG.parquet',
            'firms_AI.parquet',
            'firms_ZG.parquet',
            'firms_SH.parquet',
            'firms_VS_Bas.parquet',
            'firms_LU.parquet',
            'firms_SO.parquet',
            'firms_VS_Ober.parquet',
            'firms_SZ.parquet',
            'firms_JU.parquet',
            'firms_NE.parquet'
            ]

    for _ in range(10):
        with multiprocessing.Pool() as pool:
            pool.map(crawler_func, [files_1, files_2])

    logging.info('all done')


if __name__=='__main__':
    main()
