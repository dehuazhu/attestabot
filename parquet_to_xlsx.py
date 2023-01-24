import pandas as pd
import multiprocessing, sys

def parquet_to_xlsx(parquet_file):
    df = pd.read_parquet(parquet_file)
    df.to_excel( parquet_file.replace('.parquet', '.xlsx') )
    del df
    print(f'done with {parquet_file}')

def main():
    files = sys.argv[1:]
    with multiprocessing.Pool() as pool:
        pool.map(parquet_to_xlsx, files)

if __name__=='__main__':
    main()
