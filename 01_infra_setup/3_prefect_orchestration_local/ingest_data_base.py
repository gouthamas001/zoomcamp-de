import sys
import pandas as pd
import os
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse


def trigger_ingestion(params):
    # df = pd.read_parquet("yellow_tripdata_2023-01.parquet", nrows = 100000)
    # print(pd.io.sql.get_schema(df, name = "yellow_taxi_ddl"))
    # print(pd.io.sql.get_schema(df, name = "yellow_taxi", con=engine))
    # df.to_sql(name="yello_taxi_data", con=engine, if_exists='replace')

    # Read params from argparser object
    user = params.user_name
    password = params.password
    host = params.host_name
    port = params.port
    db = params.db_name
    table_name = params.table_name
    url = params.url
    input_file = "sample_2023.parquet"

    # input check

    if url.endswith(".parquet"):
        pass
    else:
        sys.exit()

    # download the parquet file here
    os.system(f"wget {url} -O {input_file}")

    # Reading parquet file using pyarrow
    parquet_file = pq.ParquetFile(input_file)

    # Creating db connection using sqlalchemy
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    try:
        counter = 1
        for batch in parquet_file.iter_batches(batch_size=100000):
            t_start = time()
            batch_df = batch.to_pandas()
            batch_df.to_sql(name=table_name, con=engine, if_exists="append")
            t_end = time()
            print(f"inserted chunk {counter}, took %.3f seconds" % (t_end - t_start))
            counter += 1
    except StopIteration:
        print("Failed ingestion!!!")


if __name__ == "__main__":
    # Params required --> pg hostname, username, password, port, database name, table name , file url

    parser = argparse.ArgumentParser(description="Ingest parquet file to postgres")

    parser.add_argument("--user_name", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host_name", help="host name of postgres")
    parser.add_argument("--port", help="postgres port")
    parser.add_argument("--db_name", help="database name for postgres")
    parser.add_argument("--table_name", help="table name for postgres")
    parser.add_argument("--url", help="parquet file path to to download")

    args = parser.parse_args()
    user_name = "postgres"
    password = "postgres"
    host_name = "localhost"
    port = "5432"
    db_name = "ny_taxi"
    table_name = "yellow_taxi_data"
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    trigger_ingestion(args)
