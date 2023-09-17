import pandas as pd

import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine

# df = pd.read_parquet("yellow_tripdata_2023-01.parquet", nrows = 100000)


# print(pd.io.sql.get_schema(df, name = "yellow_taxi_ddl"))


engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

engine.connect()


# print(pd.io.sql.get_schema(df, name = "yellow_taxi", con=engine))


# df.to_sql(name="yello_taxi_data", con=engine, if_exists='replace')

parquet_file = pq.ParquetFile("yellow_tripdata_2023-01.parquet")


for batch in parquet_file.iter_batches(batch_size=100000):
    t_start = time()
    batch_df = batch.to_pandas()
    batch_df.to_sql(name="yello_taxi_data", con=engine, if_exists="append")
    t_end = time()

    print("inserted another chunk.., took %.3f seconds" % (t_end - t_start))
