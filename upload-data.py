
#!/usr/bin/env python
# coding: utf-8
import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import pyarrow as pa
from pyarrow.parquet import ParquetFile

import argparse
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url


    parquet_name = "yellow_tripdata_2021-01.parquet"

    # download parquet
    os.system(f"wget {url} -O {parquet_name}")

    pf=ParquetFile(parquet_name)
    first_500k_rows = next(pf.iter_batches(batch_size = 200000)) 
    df = pa.Table.from_batches([first_500k_rows]).to_pandas() 

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.to_sql(name=table_name, con=engine, if_exists='replace')

    query = """
    select * from information_schema.tables  WHERE table_schema = 'public';
    """ 
    pd.read_sql(query, con=engine)

    query = """
    select * from yellow_taxi_trips;
    """ 
    pd.read_sql(query, con=engine)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet to Postgres')

    parser.add_argument('--user', help='user for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for writing results')
    parser.add_argument('--url', help='url for parquet file')

    args = parser.parse_args()

    main(args)