import argparse
import io
import pathlib

import pandas

import ohio.ext.pandas  # noqa

from .tool import (
    dtypes,
    free,
    loadconfig,
    loaddata,
    mprof,
    profiler,
    report,
    report_input,
    sizecheck,
    time,
)


@profiler
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
@loaddata
@dtypes
@mprof
@time
@sizecheck
def pandas_read_sql(engine, query):
    """pandas.read_sql"""
    return pandas.read_sql(query, engine, parse_dates=['as_of_date'])


@profiler
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


def main(prog=None, argv=None):
    parser = argparse.ArgumentParser(prog=prog,
                                     description="profile the codebase")
    parser.add_argument('data_path', type=pathlib.Path,
                        help="path to csv data file to load as input")
    parser.add_argument('-t', '--table', default='profiling', dest='table_name',
                        help="name to give to table in temporary database")

    args = parser.parse_args(argv)
    query = f'select * from {args.table_name}'

    loadconfig.set(args)

    report_input()
    print()

    for (index, method) in enumerate(profiler):
        if index > 0:
            print()
            free()  # NOTE: necessary?
            print()

        desc = 'begin: ' + method.__doc__ if method.__doc__ else 'begin ...'
        report(method, desc)
        method(query)


if __name__ == '__main__':
    main()
