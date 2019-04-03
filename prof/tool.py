import collections
import contextlib
import csv
import functools
import gc
import math
import re
import shutil
import timeit

import pandas
import sqlalchemy
import testing.postgresql
from memory_profiler import memory_usage

from .util import contextmanager, SharingContextDecorator, wrapper


class ConfigWrapper(SharingContextDecorator):
    """Context manager and decorator providing a configuration sentinel.

    If accessed prior to configuration being ``set``, raises
    ``RuntimeError``.

    As a context manager::

        with loadconfig as config:
            ...

    As a decorator::

        @loadconfig
        def my_func(config):
            return config.some_key

    Invoking ``my_func()`` without arguments would return the config
    value associated with ``some_key``.

    """
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


class ResultsWrapper(SharingContextDecorator):
    """Context manager and decorator providing a sentinel of profiling
    results.

    Profiling data may be recorded for a given profiler function via
    ``save``::

        @results
        def measure_something(results, func, *args, **kwargs):
            func_result = func(*args, **kwargs)
            results.save(func, some_thing=some_value)
            return func_result

    ...and retrieved via ``get``::

        @results
        def process_results(results, func):
            func_results = results.get(func)
            ...

    """
    def __init__(self):
        self.results = collections.defaultdict(lambda: collections.defaultdict(list))

    def get(self, func):
        return self.results[func.__name__]

    def save(self, func, **kwargs):
        for (key, value) in kwargs.items():
            self.results[func.__name__][key].append(value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return


results = ResultsWrapper()


class ProfilerRegistry(collections.defaultdict):
    """Profiler registry and function decorator.

    Register a function as a profiler::

        profiler = ProfilerRegistry()

        @profiler
        def profile_this():
            ...

    ...or, with a tag::

        @profiler('slow')
        def profile_slow():
            ...

    Profiler tags can also be specified by attribute::

        def profile_slow():
            ...

        profile_slow.__tag__ = 'slow'

        profiler(profile_slow)

    """
    def __init__(self):
        super().__init__(list)

    def __call__(self, func_or_tag, tag=None):
        if callable(func_or_tag):
            if tag is None:
                tag = getattr(func_or_tag, '__tag__', None)
            self[tag].append(func_or_tag)
            return func_or_tag
        elif tag:
            raise TypeError("multiple tags specified?")
        else:
            return functools.partial(self, tag=func_or_tag)

    def filtered(self):
        """Construct a dictionary of configuration-selected profilers
        categorized and sorted by tag.

        """
        # sort & ensure untagged are last
        ordered = sorted(
            self.items(),
            key=lambda pair: self._sort_key(pair[0]),
        )

        return {
            key: list(filter(self._include_profiler, profilers))
            for (key, profilers) in ordered
            if self._include_tag(key)
        }

    @staticmethod
    def _sort_key(key, last=('z' * 10_000)):
        """Sort profiler groups alphabetically by tag name.

        Ensure untagged (``None``) come *last*.

        """
        return last if key is None else key

    @staticmethod
    @loadconfig
    def _include_profiler(config, profiler):
        return not config.filters or all(filter_.search(profiler.__name__)
                                         for filter_ in config.filters)

    @staticmethod
    @loadconfig
    def _include_tag(config, tag):
        return not config.tag_filters or all(filter_.search(tag or '')
                                             for filter_ in config.tag_filters)


profiler = ProfilerRegistry()


class Reporter:
    """Report information associated with the current profiler."""

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
    """Report the size and shape of provided input data."""
    with config.data_path.open() as fd:
        header = next(fd)

        (columns,) = csv.reader((header,))
        col_count = len(columns)

        row_count = sum(1 for _row in fd)

    report(report_input, 'size data (rows x columns):',
           row_count, 'x', col_count, f'({row_count * col_count})')


report_input.__name__ = 'input'


def banner(message, fill, padding=' '):
    """Print the given message as a banner with ``fill`` characters
    filling the width of the terminal.

    """
    message = f'{padding}{message}{padding}'
    term_size = shutil.get_terminal_size()
    columns = term_size.columns
    fill_size = int((columns - len(message)) / 2)
    print(fill_size * fill + message + fill_size * fill)


def report_trial(trial_count):
    banner(f'trial {trial_count}', fill='*')


def report_tag(tag):
    banner(tag, '-')


def free():
    """Force a garbage collector run and report on the process's current
    memory use.

    """
    mem_stats = memory_usage((gc.collect,))
    mem0 = math.ceil(mem_stats[0])
    mem1 = math.ceil(mem_stats[-1])
    freed = mem0 - mem1
    report(free, 'memory (mb):', mem0, '→', mem1, f'({freed} freed)')


# @contextmanager
# def time():
#     try:
#         start = timeit.default_timer()
#         yield
#     finally:
#         report('time (s):', round(timeit.default_timer() - start, 2))

@wrapper
@results
def time(results, func, *args, **kwargs):
    """Report and record the execution time in seconds for the decorated
    function upon each invocation.

    """
    start = timeit.default_timer()
    result = func(*args, **kwargs)
    duration = round(timeit.default_timer() - start, 2)

    report('time (s):', duration)
    results.save(func, time=duration)

    return result


@wrapper
def dtypes(func, *args, **kwargs):
    """Report the Pandas dtypes of the DataFrame returned by the
    decorated function.

    """
    result = func(*args, **kwargs)

    dtypes = collections.Counter(map(str, result.dtypes.values))
    report('dtypes result:', dict(dtypes))

    return result


@wrapper
@results
def mprof(results, func, *args, **kwargs):
    """Report and record the memory usage of each invocation of the
    decorated function.

    If the function returns a Pandas DataFrame, this object's memory
    usage is reported, and subtracted from the recorded memory usage
    (overhead) of the executed function.

    """
    result = None

    def inner():
        nonlocal result
        result = func(*args, **kwargs)

    mem_stats = memory_usage((inner,))

    mem0 = math.ceil(mem_stats[0])
    mem1 = math.ceil(max(mem_stats))
    mem_added = mem1 - mem0
    report('memory used (mb):', mem0, '→', mem1, f'({mem_added} added)')

    if isinstance(result, pandas.DataFrame):
        mem_result = result.memory_usage(index=True, deep=True).sum() / 1024 / 1024
        report('memory result (mb):', math.ceil(mem_result))
    else:
        mem_result = 0

    mem_overhead = math.ceil(mem1 - mem_result)
    report('memory overhead (mb):', mem_overhead)

    results.save(func, memory=mem_overhead)

    return result


def sizecheck(func):
    """Report the size of the DataFrame returned by the decorated
    function.

    """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        df = func(*args, **kwargs)
        report('size result (cells):', df.size)
        return df

    return wrapped


@wrapper
@loadconfig
def countcheck(config, func, *args, **kwargs):
    """Report the row count of the profiling database table following
    invocation of the decorated function.

    """
    result = func(*args, **kwargs)

    (engine,) = (arg for arg in args if isinstance(arg, sqlalchemy.engine.Engine))  # FIXME?
    count = engine.execute(f'select count(1) from {config.table_name}').scalar()
    report('count table (rows):', count)

    return result


@contextmanager
def loaddb():
    """Context manager and decorator which creates a temporary
    PostgreSQL database and provides the context or function an
    associated SQLAlchemy database connection engine.

    """
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


@loadconfig
def create_table(config, header, engine):
    """Create an empty database table according to configuration and the
    input data header.

    """
    (columns,) = csv.reader((header,))
    col_defn = ', '.join(f'{column} ' + column_type(column)
                         for column in columns)

    engine.execute(f'create table {config.table_name} ({col_defn})')


@loaddb.manager
@loadconfig.manager
def loaddata(config, engine):
    """Context manager and decorator which creates the configured
    database table and populates it with input data.

    An associated SQLAlchemy database connection engine is provided to
    the context or function.

    """
    with config.data_path.open() as fd:
        header = next(fd)
        create_table(header, engine)

        with contextlib.closing(engine.raw_connection()) as conn:
            cursor = conn.cursor()
            cursor.copy_expert(
                f'copy {config.table_name} from stdin with csv',
                fd,
            )
            conn.commit()

    yield engine


@loadconfig.manager
def loadframe(config):
    """Context manager and decorator providing a Pandas DataFrame
    populated by the input data.

    """
    yield pandas.read_csv(config.data_path, index_col='entity_id', parse_dates=True)


@loadconfig.manager
def loadquery(config):
    """Context manager and decorator providing the SQL query by which
    to retrieve all data from the database.

    """
    yield f'select * from {config.table_name}'


@results
def handle_method(results, method, shared_results, result_keys):
    """Execute a profiler method (from a child process)."""
    method_results = results.get(method)
    current_lengths = [(key, len(method_results[key])) for key in result_keys]

    method()

    for (index, (key, length0)) in enumerate(current_lengths):
        key_results = method_results[key]
        assert len(key_results) == length0 + 1
        shared_results[index] = key_results[length0]


@results
def save_child_results(results, method, shared_results, result_keys):
    """Save results on behalf of the given profiler method.

    Profiler decorators are expected to store results themselves,
    in-process; this helper can be used to populate results generated
    in a child process, (having been returned by ``handle_method``).

    """
    rounded_values = (round(result, 2) for result in shared_results)
    method_results = dict(zip(result_keys, rounded_values))
    results.save(method, **method_results)
