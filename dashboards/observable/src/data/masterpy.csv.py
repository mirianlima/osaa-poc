import sys
import ibis
from ibis import _

def top_countries(t,yr=2013, db='wdi'):
    '''Identify countries with top values for 
    each indicator in selected year & source'''
    return (
        t
        .filter([_.year==yr,_.database==db])
        .aggregate(
            by='indicator_id',
            value=_.value.max(),
            top_country=_.country_id.argmax(_.value),
            label=_.indicator_label.argmax(_.value)
        )
    )

con=ibis.duckdb.connect()
master=con.read_parquet('src/data/master.parquet', "master")

master.pipe(top_countries).to_csv(sys.stdout)