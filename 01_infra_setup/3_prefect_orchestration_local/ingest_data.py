import sys
import pandas as pd
import os
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pyarrow as pa
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url):
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

    test_data = next(parquet_file.iter_batches(batch_size=100000))
    df = pa.Table.from_batches([test_data]).to_pandas()

    return df


@task(log_prints=True)
def transform(df):
    print(
        f"pre: missing passenger count before filtering: {df['passenger_count'].isin([0]).sum()}"
    )

    df = df[df["passenger_count"] != 0]

    print(
        f"post: missing passenger count after filtering: {df['passenger_count'].isin([0]).sum()}"
    )

    return df


@task(log_prints=True, retries=3)
def trigger_ingestion(params, data):
    # df = pd.read_parquet("yellow_tripdata_2023-01.parquet", nrows = 100000)
    # print(pd.io.sql.get_schema(df, name = "yellow_taxi_ddl"))
    # print(pd.io.sql.get_schema(df, name = "yellow_taxi", con=engine))
    # df.to_sql(name="yello_taxi_data", con=engine, if_exists='replace')

    # Read params from argparser object
    # user = params["user_name"]
    # password = params["password"]
    # host = params["host_name"]
    # port = params["port"]
    # db = params["db_name"]
    table_name = params["table_name"]
    # url = params["url"]

    # Creating db connection using sqlalchemy
    # engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    connection_block = SqlAlchemyConnector.load("postgre-connector")

    with connection_block.get_connection(begin=False) as engine:
        data.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        data.to_sql(name=table_name, con=engine, if_exists="append")

    # try:
    #     counter = 1
    #     for batch in parquet_file.iter_batches(batch_size=100000):
    #         t_start = time()
    #         batch_df = batch.to_pandas()
    #         batch_df.to_sql(name=table_name, con=engine, if_exists="append")
    #         t_end = time()
    #         print(f"inserted chunk {counter}, took %.3f seconds" % (t_end - t_start))
    #         counter += 1
    # except StopIteration:
    #     print("Failed ingestion!!!")


@flow(name="subflow", log_prints=True)
def tes_subflow(table_name: str):
    print(f"Testing subflow of prefect {table_name}")


@flow(name="Test ingest flow")
def main():
    # Params required --> pg hostname, username, password, port, database name, table name , file url

    # parser = argparse.ArgumentParser(description="Ingest parquet file to postgres")

    # parser.add_argument("--user_name", help="user name for postgres")
    # parser.add_argument("--password", help="password for postgres")
    # parser.add_argument("--host_name", help="host name of postgres")
    # parser.add_argument("--port", help="postgres port")
    # parser.add_argument("--db_name", help="database name for postgres")
    # parser.add_argument("--table_name", help="table name for postgres")
    # parser.add_argument("--url", help="parquet file path to to download")

    # args = parser.parse_args()
    conn_dic = {
        "user_name": "postgres",
        "password": "postgres",
        "host_name": "localhost",
        "port": "5432",
        "db_name": "ny_taxi",
        "table_name": "yellow_taxi_data",
        "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    }

    tes_subflow(conn_dic["table_name"])
    sample_raw_data = extract_data(conn_dic["url"])
    transformed_data = transform(sample_raw_data)

    trigger_ingestion(conn_dic, transformed_data)


if __name__ == "__main__":
    main()
