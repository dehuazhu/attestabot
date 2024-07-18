import os, logging, jsonlines, multiprocessing
from glob import glob
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from attestabot.spiders.MoneyhouseSpider import MoneyhouseSpider
from pdb import set_trace

STARTING_LINE         = 0
MAX_REQUESTS_PER_FILE = 500
INPUT_FOLDER  = 'firms_zefix'
OUTFILE_RAW   = 'moneyhouse_data.jl'
OUTPUT_FOLDER = 'firms_moneyhouse'
LOGFILE       = 'moneyhouse_scraper.log'

def write_raw_output_to_files(parquet_file):
    df = pd.read_parquet(parquet_file)
    with jsonlines.open(OUTFILE_RAW) as jl_file:
        rows = (pd.Series(row) for row in jl_file if row['parquet_file']==parquet_file)
        df_moneyhouse = pd.concat(rows, axis=1).T.drop(columns='parquet_file').set_index('df_index')
    df_moneyhouse.rename(columns={'checked_on' : 'moneyhouse_checked_on'}, inplace=True)
    new_df = df.join(df_moneyhouse)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    outfile = os.path.basename(parquet_file)
    outfile = os.path.join(OUTPUT_FOLDER, outfile)
    new_df.to_parquet( outfile )
    new_df.to_excel( outfile.replace('.parquet','.xlsx') )
    del df, df_moneyhouse, new_df


def main():
    files = glob( os.path.join(INPUT_FOLDER, '*.parquet') )

    crawler_settings = get_project_settings()
    crawler_settings['LOG_ENABLED']    = True
    #crawler_settings['LOG_FILE']       = LOGFILE
    crawler_settings['LOG_LEVEL']      = 'INFO'
    crawler_settings['DOWNLOAD_DELAY'] = 1
    crawler_settings['FEEDS']          = {
            OUTFILE_RAW : {
                'format'    : 'jsonlines',
                'encoding'  : 'utf8',
                'overwrite' : False,
                },
            }

    crawler_process = CrawlerProcess(crawler_settings)
    for parquet_file in files:
        crawler_process.crawl(
                MoneyhouseSpider,
                parquet_file          = parquet_file,
                #starting_line         = STARTING_LINE,
                #max_requests_per_file = MAX_REQUESTS_PER_FILE
                )

    logging.info(f'Starting crawl jobs')
    crawler_process.start()

    logging.info(f'Done crawling, will now write raw results to Excel files')

    with multiprocessing.Pool() as pool:
        pool.map(write_raw_output_to_files, files)

if __name__=='__main__':
    main()
