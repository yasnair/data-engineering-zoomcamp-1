
# Imports
from datetime import timedelta
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    #Download the csv
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

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

    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):

    print("Connecting to the DB")
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        #n=0 takes only the header of the table and it's to create it in the DB before insert data
        #if we do df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace') it will insert all records 
        #as we want just create the table we use:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


        #Now we insert the first 100000 record.
        #%time tell us how much time it took run insert the records
        df.to_sql(name=table_name, con=engine, if_exists='append')
    
@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count:{df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] !=0]
    print(f"post: missing passenger count:{df['passenger_count'].isin([0]).sum()}")
    return df


@flow(name="Ingest Flow")
def main_flow():             
    table_name="yellow_taxi_trips1" 
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow()





