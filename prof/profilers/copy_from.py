import io

import pandas
import sqlalchemy

import prof.tool
from prof.tool import (
    dtypes,
    loaddata,
    loadquery,
    mprof,
    sizecheck,
    time,
)


profiler = prof.tool.profiler('copy from database to dataframe')


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def ohio_pg_copy_from_1(engine, query):
    """pg_copy_from(buffer_size=1) {COPY → PipeTextIO → pandas.read_csv}"""
    return pandas.DataFrame.pg_copy_from(query,
                                         engine,
                                         parse_dates=['as_of_date'],
                                         buffer_size=1)


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def ohio_pg_copy_from_10(engine, query):
    """pg_copy_from(buffer_size=10) {COPY → PipeTextIO → pandas.read_csv}"""
    return pandas.DataFrame.pg_copy_from(query,
                                         engine,
                                         parse_dates=['as_of_date'],
                                         buffer_size=10)


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def ohio_pg_copy_from_10_stream_results(engine, query):
    """pg_copy_from(buffer_size=10) {stream_results | COPY → PipeTextIO → pandas.read_csv}"""
    engine1 = sqlalchemy.create_engine(engine.url, execution_options=dict(stream_results=True,
                                                                          max_row_buffer=10))
    try:
        return pandas.DataFrame.pg_copy_from(query,
                                             engine1,
                                             parse_dates=['as_of_date'],
                                             buffer_size=10)
    finally:
        engine1.dispose()


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def ohio_pg_copy_from_100(engine, query):
    """pg_copy_from(buffer_size=100) {COPY → PipeTextIO → pandas.read_csv}"""
    return pandas.DataFrame.pg_copy_from(query,
                                         engine,
                                         parse_dates=['as_of_date'],
                                         buffer_size=100)


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def ohio_pg_copy_from_1000(engine, query):
    """pg_copy_from(buffer_size=1000) {COPY → PipeTextIO → pandas.read_csv}"""
    return pandas.DataFrame.pg_copy_from(query,
                                         engine,
                                         parse_dates=['as_of_date'],
                                         buffer_size=1000)


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def pandas_read_csv_stringio(engine, query):
    """COPY → StringIO → pandas.read_csv"""
    connection = engine.raw_connection()
    cursor = connection.cursor()
    buffer = io.StringIO()
    cursor.copy_expert(
        f'COPY ({query}) TO STDOUT WITH CSV HEADER',
        buffer,
    )
    buffer.seek(0)
    return pandas.read_csv(
        buffer,
        parse_dates=['as_of_date'],
    )


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def pandas_read_sql_chunks_100_stream_results(engine, query):
    """pandas.read_sql(chunksize=100) {stream_results}"""
    engine1 = sqlalchemy.create_engine(engine.url, execution_options=dict(stream_results=True,
                                                                          max_row_buffer=100))
    try:
        chunks = pandas.read_sql(query, engine1, parse_dates=['as_of_date'], chunksize=100)
        return pandas.concat(chunks, copy=False)
    finally:
        engine1.dispose()


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def pandas_read_sql_chunks_100(engine, query):
    """pandas.read_sql(chunksize=100)"""
    chunks = pandas.read_sql(query, engine, parse_dates=['as_of_date'], chunksize=100)
    return pandas.concat(chunks, copy=False)


@profiler
@loadquery
@loaddata
@dtypes
@mprof
@time
@sizecheck
def pandas_read_sql(engine, query):
    """pandas.read_sql"""
    return pandas.read_sql(query, engine, parse_dates=['as_of_date'])
