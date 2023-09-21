from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import ssl


ssl._create_default_https_context = ssl._create_unverified_context


@task(retries=3)
def fetch_file(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""

    df = pd.read_csv(url)

    return df


@task(log_prints=True)
def data_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Fix Data type issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"total no.of rows : {len(df)}")
    return df


@task()
def write_to_local(df: pd.DataFrame, colour: str, dataset_file: str) -> Path:
    """write dataframe into a parquet file"""
    path = Path(f"data/{colour}/{dataset_file}.parquet")

    df.to_parquet(path, compression="snappy")

    return path


@task()
def write_to_gcs(path: Path) -> None:
    """Upload parquet file into GCS bucket"""

    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=f"data/{path}")


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL fucntion"""
    colour = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    print(dataset_file)
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch_file(dataset_url)

    cleaned_data = data_cleaning(df)

    path = write_to_local(cleaned_data, colour, dataset_file)

    write_to_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
