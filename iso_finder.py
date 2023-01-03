from glob import glob
import pandas as pd
import sys, os, logging, jsonlines, multiprocessing
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from attestabot.spiders.IsoFinder import IsoFinder
from pdb import set_trace

OUTFILE_RAW   = 'iso_finder_raw.jl'
INPUT_FOLDER  = 'firms_website_finder'
OUTPUT_FOLDER = 'firms_iso_finder'
LOGFILE       = 'iso_finder.log'

def sort_dataframes_and_save(parquet_infile, df_iso_finder):
    today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    df_website_finder = pd.read_parquet(parquet_infile).query('url_exists=="TRUE"').rename({'url': 'company_homepage'}, axis='columns')
    new_columns = []
    for column in df_website_finder.columns:
        if column in ('url_exists', 'url_checked_on'):
            continue
        if column not in df_iso_finder:
            new_columns += [column]
            df_iso_finder[column] = None
    for idx, (name,company_homepage) in df_iso_finder[['name','company_homepage']].drop_duplicates().iterrows():
        new_columns_values = df_website_finder.query('name==@name & company_homepage==@company_homepage')
        if len(new_columns_values)>1: logging.warning(f'Problem with {name}, url: {company_homepage}')
        df_iso_finder.loc[(df_iso_finder['name']==name) & (df_iso_finder['company_homepage']==company_homepage), new_columns] = new_columns_values.iloc[0][new_columns].values

    df_iso_finder.sort_values(by=['name','company_homepage'], ignore_index=True, inplace=True)
    df_iso_finder = df_iso_finder[['name', 'ehraid', 'uid', 'uidFormatted', 'chid', 'chidFormatted', 'legalSeatId', 'legalSeat', 'cantonId', 'canton', 'registerOfficeId', 'legalFormId', 'status', 'rabId', 'shabDate', 'deleteDate', 'cantonalExcerptWeb', 'company_homepage', 'suburl_with_iso_info', 'suburl_has_iso_file', 'suburl_is_iso_file', 'suburl_has_keyword', 'suburl_has_logo']].copy()
    df_iso_finder['iso_checked_on'] = today
    df_iso_finder['iso_certificate_parsed'] = 'FALSE'

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    outfile = os.path.basename(parquet_infile)
    outfile = os.path.join(OUTPUT_FOLDER, outfile)
    df_iso_finder.to_parquet( outfile )
    #df_iso_finder.to_excel( outfile.replace('.parquet','.xlsx') )
    del df_iso_finder, df_website_finder


def crawler_func(files):
    suffix = 1 if 'firms_FR.parquet' in files else 2
    outfile = OUTFILE_RAW.replace('.jl',f'_{suffix}.jl')
    files = [os.path.join(INPUT_FOLDER, parquet_file) for parquet_file in files]
    crawler_settings = get_project_settings()
    #crawler_settings['LOG_FILE']             = LOGFILE.replace('.log',f'_{suffix}.log')
    crawler_settings['LOG_ENABLED']          = True
    crawler_settings['LOG_FILE_APPEND']      = False
    crawler_settings['LOG_LEVEL']            = 'INFO'
    #crawler_settings['LOG_LEVEL']            = 'DEBUG'
    crawler_settings['FEEDS']                = {
            outfile : {'format': 'jsonlines'}
            }

    crawler_process = CrawlerProcess(crawler_settings)
    for parquet_file in files:
        crawler_process.crawl(IsoFinder, parquet_file=parquet_file)

    logging.info(f'Starting crawl jobs')
    crawler_process.start()

    logging.info(f'Done crawling part {suffix}, will now write raw results to Excel files')

    with jsonlines.open(outfile) as jl_file:
         df = pd.concat((pd.Series(row) for row in jl_file), axis=1).T.drop_duplicates()

    df_dict = {
            parquet_file : df.query('parquet_file==@parquet_file').drop(columns='parquet_file')
            for parquet_file in files
            }
    for parquet_infile, df in df_dict.items():
        sort_dataframes_and_save(parquet_infile, df)
    #with multiprocessing.Pool() as pool:
    #    pool.starmap(sort_dataframes_and_save, df_dict.items())
    logging.info(f'finished part {suffix}')


def main():
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

    with multiprocessing.Pool() as pool:
        pool.map(crawler_func, [files_1, files_2])

    logging.info('all done')


if __name__=='__main__':
    main()
