'''
Helper script to merge several dataframes in  pickle files to a single excel table.
Usage: python merge_tables.py 'firms_zefix/*pkl'
'''
import pandas as pd
import sys, os
from glob import glob
from pdb import set_trace

def merge(files, outfile=None):
    df = pd.concat((pd.read_pickle(file) for file in files), ignore_index=True) # merge everything into a single dataframe
    if outfile is None:
        outfile = os.path.join(os.path.dirname(files[0]), 'all.xlsx')

    df.to_excel(outfile)

if __name__=='__main__':
    files = glob(sys.argv[1])
    if len(sys.argv)>2:
        outfile = sys.argv[-1]
    else:
        outfile = None

    merge(files, outfile)
