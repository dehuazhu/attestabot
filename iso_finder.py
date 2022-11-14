from glob import glob
import pandas as pd
import sys, os, logging, jsonlines, multiprocessing
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from attestabot.spiders.IsoFinder import IsoFinder
from pdb import set_trace


def merge_dataframes_and_save(pkl_outfile, to_merge):
    df_files_kws = pd.merge(to_merge['files'], to_merge['kws'], how='outer').drop_duplicates()
    df_merged = pd.merge(df_files_kws, to_merge['logos'], how='outer').drop_duplicates()
    df_merged = pd.concat((df_merged[df_merged.has_file.isna()], df_merged[df_merged.has_file.notna()].drop_duplicates(subset='has_file')))
    df_merged[df_merged.isna()] = 'FALSE'
    df_merged.sort_values(by='name', ignore_index=True, inplace=True)
    #df_merged.to_pickle( pkl_outfile )
    df_merged.to_excel( pkl_outfile.replace('.pkl','.xlsx') )
    del to_merge


def main():
    LOGFILE = 'iso_finder.log'
    crawler_settings = get_project_settings()
    #crawler_settings['LOG_FILE']             = LOGFILE
    #crawler_settings['LOG_ENABLED']          = False
    #crawler_settings['LOG_FILE_APPEND']      = False
    #crawler_settings['LOG_LEVEL']            = 'INFO'
    crawler_settings['FEEDS']                = {
            'iso_finder_raw.jl' : {'format': 'jsonlines'}
            }
    crawler_process = CrawlerProcess(crawler_settings)

    #files = glob( os.path.join('firms','firms*pkl') )
    files = ['test.pkl']
    for pkl_file in files:
        crawler_process.crawl(IsoFinder, pkl_file=pkl_file)

    logging.info('Starting crawl jobs')
    crawler_process.start()

    logging.info('Done crawling, will now write raw results to Excel files')

    found_files = []
    found_kws   = []
    found_logos = []

    with jsonlines.open('iso_finder_raw.jl') as jl_file:
        for hit in jl_file:
            #print(hit)
            if hit['payload']=='has_keyword':
                found_kws += [pd.Series(hit)]
            elif hit['payload']=='has_logo':
                found_logos += [pd.Series(hit)]
            else:
                found_files += [pd.Series(hit)]
    df_files = pd.concat(found_files, axis=1).T.rename({'payload' : 'has_file'}, axis='columns')
    df_kws   = pd.concat(found_kws, axis=1).T.rename({'payload' : 'has_keyword'}, axis='columns')
    df_logos = pd.concat(found_logos, axis=1).T.rename({'payload' : 'has_logo'}, axis='columns')

    del found_files, found_kws, found_logos

    df_kws.has_keyword = 'TRUE'
    df_logos.has_logo  = 'TRUE'

    df_dict = {
            pkl_file : {
                'files' : df_files.query('pkl_file==@pkl_file').drop(columns='pkl_file'),
                'kws'   : df_kws.query('pkl_file==@pkl_file').drop(columns='pkl_file'),
                'logos' : df_logos.query('pkl_file==@pkl_file').drop(columns='pkl_file'),
                }
            for pkl_file in files
            }
    for pkl_outfile, df in df_dict.items():
        merge_dataframes_and_save(pkl_outfile, df)
    #with multiprocessing.Pool() as pool:
    #    pool.starmap(merge_dataframes_and_save, df_dict.items())
    logging.info('all done')


if __name__=='__main__':
    main()
