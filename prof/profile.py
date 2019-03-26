#!/usr/bin/env python3
import collections
import contextlib
import csv
import functools
import gc
import io
import math
import os
import re
import timeit

import pandas
import sqlalchemy
import testing.postgresql
from memory_profiler import memory_usage

import ohio.ext.pandas  # noqa


DATA_PATH = os.getenv('DATA_PATH')

if not DATA_PATH:
    raise EnvironmentError('supply DATA_PATH')

TABLE_NAME = 'profiling'

QUERY = f'select * from {TABLE_NAME}'


# utils #

# better contextmanager #

class _GeneratorContextManager(contextlib._GeneratorContextManager):

    def __call__(self, func):
        @functools.wraps(func)
        def inner(*args, **kwds):
            # unlike the builtin, we'll pass the result of __enter__ to the
            # decorated function as an initial argument
            with self._recreate_cm() as ctx:
                ctx_args = () if ctx is None else (ctx,)
                return func(*(ctx_args + args), **kwds)
        return inner


class Wrapper:

    def __init__(self, func):
        functools.update_wrapper(self, func)


class contextmanager(Wrapper):
    """@contextmanager decorator.

    Typical usage:

        @contextmanager
        def some_generator(<arguments>):
            <setup>
            try:
                yield <value>
            finally:
                <cleanup>

    This makes this:

        with some_generator(<arguments>) as <variable>:
            <body>

    equivalent to this:

        <setup>
        try:
            <variable> = <value>
            <body>
        finally:
            <cleanup>

    """
    def __call__(self, *args, **kwds):
        # unlike the builtin, we'll enable decoration without
        # invocation of the decorator
        if not kwds and len(args) == 1 and callable(args[0]):
            return _GeneratorContextManager(self.__wrapped__, (), kwds)(args[0])
        return _GeneratorContextManager(self.__wrapped__, args, kwds)

    def manager(self, func):
        """construct contextmanager that requires (composes) another."""
        inner = self.__class__(func)

        @functools.wraps(func)
        def composition(*args, **kwds):
            with self() as ctx0:
                with inner(ctx0) as ctx1:
                    yield ctx1

        return self.__class__(composition)


def report_input():
    with open(DATA_PATH) as fd:
        header = next(fd)

        (columns,) = csv.reader((header,))
        col_count = len(columns)

        row_count = sum(1 for _row in fd)

    report(report_input, 'size data (rows x columns):',
           row_count, 'x', col_count, f'({row_count * col_count})')


report_input.__name__ = 'input'


@contextmanager
def time():
    try:
        start = timeit.default_timer()
        yield
    finally:
        report('time (s):', round(timeit.default_timer() - start, 2))


def histogram(stream):
    counts = collections.defaultdict(int)
    for item in stream:
        counts[item] += 1
    return counts


class dtypes(Wrapper):

    def __call__(self, *args, **kwargs):
        result = self.__wrapped__(*args, **kwargs)

        dtypes = histogram(map(str, result.dtypes.values))
        report('dtypes result:', dict(dtypes))

        return result


class mprof(Wrapper):

    def __call__(self, *args, **kwargs):
        result = None

        def inner():
            nonlocal result
            result = self.__wrapped__(*args, **kwargs)

        mem_stats = memory_usage((inner,))

        mem0 = math.ceil(mem_stats[0])
        mem1 = math.ceil(max(mem_stats))
        mem_added = mem1 - mem0
        report('memory used (mb):', mem0, '→', mem1, f'({mem_added} added)')

        mem_result = result.memory_usage(index=True, deep=True).sum() / 1024 / 1024
        report('memory result (mb):', math.ceil(mem_result))

        report('memory overhead (mb):', math.ceil(mem1 - mem_result))

        return result


def free():
    mem_stats = memory_usage((gc.collect,))
    mem0 = math.ceil(mem_stats[0])
    mem1 = math.ceil(mem_stats[-1])
    freed = mem0 - mem1
    report(free, 'memory (mb):', mem0, '→', mem1, f'({freed} freed)')


def sizecheck(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        df = func(*args, **kwargs)
        report('size result (cells):', df.size)
        return df

    return wrapped


@contextmanager
def loaddb():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sqlalchemy.create_engine(postgresql.url())
        try:
            yield engine
        finally:
            engine.dispose()


COL_TYPE_PATTERNS = (
    ('timestamp', re.compile(r'(^|[-_ ])date([-_ ]|$)')),
    ('double precision', re.compile('')),  # match all
)


def column_type(column, default='varchar'):
    for (col_type, col_pattern) in COL_TYPE_PATTERNS:
        if col_pattern.search(column):
            return col_type

    return default


@loaddb.manager
def loaddata(engine):
    with open(DATA_PATH) as fd:
        header = next(fd)

        (columns,) = csv.reader((header,))
        col_defn = ', '.join(f'{column} ' + column_type(column)
                             for column in columns)

        engine.execute(f'create table {TABLE_NAME} ({col_defn})')

        with contextlib.closing(engine.raw_connection()) as conn:
            cursor = conn.cursor()
            cursor.copy_expert(
                f'copy {TABLE_NAME} from stdin with csv',
                fd,
            )
            conn.commit()

    yield engine


class ProfilerRegistry(list):

    def __call__(self, func):
        self.append(func)
        return func


profiler = ProfilerRegistry()


class Reporter:

    def __init__(self):
        self.last_func = None

    def __call__(self, *args, **kwargs):
        if callable(args[0]):
            (func, *args) = args
            self.last_func = func.__name__

        print(f'[{self.last_func}]', *args, **kwargs)


report = Reporter()


# methods to profile #

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


if __name__ == '__main__':
    report_input()
    print()

    for (index, method) in enumerate(profiler):
        if index > 0:
            print()
            free()  # NOTE: necessary?
            print()

        desc = 'begin: ' + method.__doc__ if method.__doc__ else 'begin ...'
        report(method, desc)
        method(QUERY)


# %memit print(timeit("print(pandas.read_sql(query, engine, parse_dates=['as_of_date']).size)", number=1, globals=globals()))  # noqa
# %memit print(timeit("print(pandas.DataFrame.pg_copy_from(query, engine, parse_dates=['as_of_date']).size)", number=1, globals=globals()))  # noqa


# def buffer_copy():
#     connection = engine.raw_connection()
#     cursor = connection.cursor()
#     buffer = io.StringIO()
#     cursor.copy_expert(
#         f'COPY ({query}) TO STDOUT WITH CSV HEADER',
#         buffer,
#     )
#     buffer.seek(0)
#     df = pandas.read_csv(
#         buffer,
#         parse_dates=['as_of_date'],
#     )
#     print(df.size)


# %memit print(timeit(buffer_copy, number=1, globals=globals()))
