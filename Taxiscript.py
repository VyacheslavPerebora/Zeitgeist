import os
from typing import List
import sqlite3
import luigi
import pandas as pd
import requests
from luigi.format import Nop
from luigi.contrib.sqla import CopyToTable
import sqlalchemy as s

def download_dataset(filename: str) -> requests.Response:
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response

def get_filename(year: int, month: int) -> str:
    return f'yellow_tripdata_{year}-{month:02}.parquet'



def group_by_pickup_date(file_object, group_by='pickup_date', metrics: List[str] = None) -> pd.DataFrame:
    if metrics is None:
        metrics = ['tip_amount', 'total_amount']

    df = pd.read_parquet(file_object, engine='fastparquet')
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
    df = df.groupby(group_by)[metrics].sum().reset_index()
    return df


class DownloadTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def filename(self):
        return get_filename(self.year, self.month)

    def run(self):

        self.output().makedirs()  # in case path does not exist
        response = download_dataset(self.filename)         #response = download_dataset(self.filename).content
        with self.output().open(mode='w') as f:
            #f.write(response)
            for chunk in response.iter_content(chunk_size=512):     #decode_unicode=True
                f.write(chunk)        #.encode('utf-8')

    def output(self):
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', self.filename), format=Nop)



class AggregateTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return DownloadTaxiTripTask(year=self.year, month=self.month)

    def run(self):
        try:

            with self.input().open(mode='r') as inp, self.output().open(mode='w') as output:
                self.output().makedirs()
                df = group_by_pickup_date(inp)
                df.to_parquet(output, index=False)
        except FileNotFoundError:
            pass

    def output(self):
        filename = get_filename(self.year, self.month)[:-8]
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', f'{filename}-agg.parquet'), format=Nop)




class CopyTaxiTripData2SQLite(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    table = "nyc_trip_agg_data"
    connection_string = "sqlite:///sqlite.db"

    columns = [
        (["pickup_date", s.Date()], {}),
        (["tip_amount", s.Numeric(2)], {}),
        (["total_amount", s.Numeric(2)], {}),
    ]

    def requires(self):
        return AggregateTaxiTripTask(year=self.year, month=self.month)

    def rows(self):
        with self.input().open() as parquet_file:
            # use pandas not to deal with type conversions
            df = pd.read_parquet(parquet_file)#, parse_dates=['pickup_date'])
            rows = df.to_dict(orient='split')['data']
            return rows



if __name__ == '__main__':
    #luigi.run()
    conn = sqlite3.connect('sqlite.db')
    cur = conn.cursor()
    for row in cur.execute(
        """
        select pickup_date,
               tip_amount,
               total_amount
        from nyc_trip_agg_data
        where tip_amount = (select max(tip_amount) from nyc_trip_agg_data);
        """
    ):
        print(row)
    conn.close()