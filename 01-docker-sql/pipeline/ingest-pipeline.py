#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# df.dtypes.astype(str).to_dict() -> returns the dtype in json

def run():
    year = 2021
    month = 1
    prefix = f"https://github.com/DataTalksClub/nyc-tlc-data/releases"
    url = f"{prefix}/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz"

    types = {
    'VendorID': 'Int64',
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'RatecodeID': 'Int64',
    'store_and_fwd_flag': 'string',
    'PULocationID': 'Int64',
    'DOLocationID': 'Int64',
    'payment_type': 'Int64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'congestion_surcharge': 'float64'
    }

    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    sql='postgresql'
    user='root'
    password='root'
    host='localhost'
    port=5432
    database='ny_taxi'
    chunksize=100000
    tablename='yellow_taxi_data'
    engine = create_engine(f"{sql}://{user}:{password}@{host}:{port}/{database}")


    # Define an iterator
    df_iter = pd.read_csv(url,dtype=types, parse_dates=parse_dates, iterator=True, chunksize=chunksize)

    # print(pd.io.sql.get_schema(df_iter, name=tablename, con=engine))

    first = True
    for chunk in tqdm(df_iter,total=14):
        if first:
            # Create the table
            chunk.head(0).to_sql(name=tablename, con=engine, if_exists='replace')
            first = False
        # ingest data
        chunk.to_sql(name=tablename, con=engine, if_exists='append')



if __name__ == '__main__':
    run()
