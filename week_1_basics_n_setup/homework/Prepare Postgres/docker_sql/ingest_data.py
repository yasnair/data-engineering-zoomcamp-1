#!/usr/bin/env python
# coding: utf-8

# Imports
import argparse
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
from urllib.parse import urlparse

def main(params):
    # get parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #Download the csv
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    print("Connecting to the DB")
    # create postgres conection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Loading data.......")
    # Read the csv file
    #chunksize=10000 will insert into the DB into smaller batches
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    #Get the next element in the iterator
    df =next(df_iter) 

    # Convert dates fiels to a datatime object
    for col in df.columns:
        if 'datetime' in col:
            df[col]  = pd.to_datetime(df[col])

    #n=0 takes only the header of the table and it's to create it in the DB before insert data
    #if we do df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace') it will insert all records 
    #as we want just create the table we use:
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    #Now we insert the first 100000 record.
    #%time tell us how much time it took run insert the records
    df.to_sql(name=table_name, con=engine, if_exists='append')
    try:
        #We insert the rest of the data
        while True:

            t_start = time()

            df = next(df_iter)

            # Convert dates fiels to a datatime object
            for col in df.columns:
                if 'datetime' in col:
                    df[col]  = pd.to_datetime(df[col])

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunck..., took %.3f second' %(t_end - t_start)) #%.3f indicates that the value is a float with 3 decimal places
    except:
        print("An exception occurred: No more data to iterate.")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    
    main(args)



