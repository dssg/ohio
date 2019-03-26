import contextlib
import csv
import functools
import gc
import math
import re
import timeit

import sqlalchemy
import testing.postgresql
from memory_profiler import memory_usage

from .util import contextmanager, histogram, SharingContextDecorator, Wrapper


class ConfigWrapper(SharingContextDecorator):

    def __init__(self, config=None):
        self.config = config

    def set(self, config):
        self.config = config

    def __enter__(self):
        if self.config is None:
            raise RuntimeError("config unset")
        return self.config

    def __exit__(self, exc_type, exc_value, traceback):
        return


loadconfig = ConfigWrapper()


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


@loadconfig
def report_input(config):
    with config.data_path.open() as fd:
        header = next(fd)

        (columns,) = csv.reader((header,))
        col_count = len(columns)

        row_count = sum(1 for _row in fd)

    report(report_input, 'size data (rows x columns):',
           row_count, 'x', col_count, f'({row_count * col_count})')


report_input.__name__ = 'input'


def free():
    mem_stats = memory_usage((gc.collect,))
    mem0 = math.ceil(mem_stats[0])
    mem1 = math.ceil(mem_stats[-1])
    freed = mem0 - mem1
    report(free, 'memory (mb):', mem0, '→', mem1, f'({freed} freed)')


@contextmanager
def time():
    try:
        start = timeit.default_timer()
        yield
    finally:
        report('time (s):', round(timeit.default_timer() - start, 2))


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
@loadconfig
def loaddata(config, engine):
    with config.data_path.open() as fd:
        header = next(fd)

        (columns,) = csv.reader((header,))
        col_defn = ', '.join(f'{column} ' + column_type(column)
                             for column in columns)

        engine.execute(f'create table {config.table_name} ({col_defn})')

        with contextlib.closing(engine.raw_connection()) as conn:
            cursor = conn.cursor()
            cursor.copy_expert(
                f'copy {config.table_name} from stdin with csv',
                fd,
            )
            conn.commit()

    yield engine
