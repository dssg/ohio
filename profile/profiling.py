%load_ext memory_profiler

import io
import os
from timeit import timeit

import pandas
import ohio.ext.pandas

import sqlalchemy


db_uri = os.getenv('DATABASE_URL')
engine = sqlalchemy.create_engine(db_uri)

query = 'select * from features.risks_aggregation_imputed'

%memit print(timeit("print(pandas.read_sql(query, engine, parse_dates=['as_of_date']).size)", number=1, globals=globals()))
%memit print(timeit("print(pandas.DataFrame.pg_copy_from(query, engine, parse_dates=['as_of_date']).size)", number=1, globals=globals()))


def buffer_copy():
    connection = engine.raw_connection()
    cursor = connection.cursor()
    buffer = io.StringIO()
    cursor.copy_expert(
        f'COPY ({query}) TO STDOUT WITH CSV HEADER',
        buffer,
    )
    buffer.seek(0)
    df = pandas.read_csv(
        buffer,
        parse_dates=['as_of_date'],
    )
    print(df.size)


%memit print(timeit(buffer_copy, number=1, globals=globals()))
