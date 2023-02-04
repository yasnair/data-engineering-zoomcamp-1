from datetime import timedelta
from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    #if randint(0, 1) > 0: #waiting time"
    #    raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df= pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # Convert dates fiels to a datatime object
    df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out  locally as parquet file"""
    outdir = f'./data/{color}'
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    path      = Path(os.path.join(outdir, f"{dataset_file}.parquet"))

    try :
        df.to_parquet(path, compression="gzip")
    except OSError as error :
        print(error)
    
    return path

@flow()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block   = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL fuction"""
    dataset_file    = f"{color}_tripdata_{year}-{month:02}"
    dataset_url     = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df              = fetch(dataset_url)
    df_clean        = clean(df)
    path            = write_local(df_clean, color, dataset_file)
    write_gcs(path)



@flow()
def etl_parent_flow(
    months: list[int] = [1,2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    color           = "yellow"
    year            = 2019
    months          = [2,3]
    etl_parent_flow(months, year, color)