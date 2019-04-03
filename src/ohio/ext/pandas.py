"""ohio extensions for pandas

To enable, simply import this module anywhere in your project,
(most likely -- just once, in its root module)::

    >>> import ohio.ext.pandas

For example, if you have just one module -- in there -- or, in a Python
package::

    ohio/
        __init__.py
        baseio.py
        ...

then in its ``__init__.py``, to ensure that extensions are loaded before
your code, which uses them, is run.

NOTE: These extensions are intended for Pandas, and attempt to
``import pandas``. Pandas must be available (installed) in your
environment.

"""
import contextlib
import csv
import functools
import random

import ohio
import pandas


def _get_cursor_name(prefix):
    return '{}{:0>3}'.format(prefix, random.randint(0, 999))


@pandas.api.extensions.register_dataframe_accessor('pg_copy_to')
class DataFramePgCopyTo:
    """Copy ``DataFrame`` to database table via PostgreSQL ``COPY``.

    ``ohio.PipeTextIO`` enables the direct, in-process "piping" of
    ``DataFrame`` CSV into the "standard input" of the PostgreSQL
    ``COPY`` command, for quick, memory-efficient database persistence,
    (and without the needless involvement of the local file system).

    For example, given a SQLAlchemy database connection engine and a
    Pandas ``DataFrame``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('sqlite://', echo=False)

        >>> df = pandas.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})

    We may simply invoke the ``DataFrame``'s ohio extension method,
    ``pg_copy_to``::

        >>> df.pg_copy_to('users', engine)

    ``pg_copy_to`` supports all the same parameters as ``to_sql``,
    (excepting parameter ``method``).

    In addition to the signature of ``to_sql``, ``pg_copy_to`` accepts
    the optimization parameter ``buffer_size``, which controls the
    maximum number of CSV-encoded write results to hold in memory prior
    to their being read into the database. Depending on use-case,
    increasing this value may speed up the operation, at the cost of
    additional memory -- and vice-versa. ``buffer_size`` defaults to
    ``100``.

    The optimization parameter ``iter_size`` may also be specified,
    which instructs ``pg_copy_to`` to use the named cursor feature of
    ``psycopg2``, (and so a server-size cursor), with the given *cursor*
    buffer ("iter") size. This parameter allows the user even lower-
    level control, over the number of database requests that are made,
    and the number of database results to hold in memory prior to their
    being read from the cursor.

    **Note**: This level of optimizational tuning, (especially
    ``iter_size``), *may not* produce a significant, measurable result
    for your task. For that matter, you might find the SQLAlchemy
    connection ``execution_option`` -- ``stream_results=True`` -- at
    least as useful. Consider performance profiling your task.

    """
    def __init__(self, data_frame):
        self.data_frame = data_frame

    @functools.wraps(pandas.DataFrame.to_sql)
    def __call__(self, *args, buffer_size=100, iter_size=None, **kwargs):
        to_sql_method = functools.partial(to_sql_method_pg_copy_to,
                                          buffer_size=buffer_size,
                                          iter_size=iter_size)
        self.data_frame.to_sql(
            *args,
            method=to_sql_method,
            **kwargs,
        )

        # NOTE: this was previously implemented as follows;
        # (but, this couldn't easily support all of to_sql's features):
        #
        # with ohio.pipe_text(self.data_frame.to_csv) as pipe, \
        #         contextlib.closing(engine.raw_connection()) as conn:
        #     cursor = conn.cursor()
        #
        #     cursor.copy_expert(
        #         'COPY {name} FROM STDIN WITH CSV HEADER'.format(name=name),
        #         pipe,
        #     )


def _write_csv(buffer, rows):
    writer = csv.writer(buffer)
    writer.writerows(rows)


def to_sql_method_pg_copy_to(table, conn, keys, data_iter, buffer_size, iter_size):
    columns = ', '.join('"{}"'.format(key) for key in keys)
    if table.schema:
        table_name = '{}.{}'.format(table.schema, table.name)
    else:
        table_name = table.name

    sql = 'COPY {table_name} ({columns}) FROM STDIN WITH CSV'.format(
        table_name=table_name,
        columns=columns,
    )

    if iter_size:
        cursor_name = _get_cursor_name('df_copy_to_')
    else:
        cursor_name = None

    # Note: this could use a csv stream rather than csv.writer
    with ohio.pipe_text(_write_csv,
                        data_iter,
                        buffer_size=buffer_size) as pipe, \
            conn.connection.cursor(name=cursor_name) as cursor:

        if iter_size:
            cursor.itersize = iter_size

        cursor.copy_expert(sql, pipe)


def data_frame_pg_copy_from(sql, engine,
                            index_col=None, parse_dates=False, columns=None,
                            dtype=None, nrows=None,
                            buffer_size=100, iter_size=None):
    """Construct ``DataFrame`` from database table or query via
    PostgreSQL ``COPY``.

    ``ohio.PipeTextIO`` enables the direct, in-process "piping" of
    the PostgreSQL ``COPY`` command into Pandas ``read_csv``, for quick,
    memory-efficient construction of ``DataFrame`` from database, (and
    without the needless involvement of the local file system).

    For example, given a SQLAlchemy database connection engine::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('sqlite://', echo=False)

    We may simply invoke the ``DataFrame``'s ohio extension method,
    ``pg_copy_from``::

        >>> df = DataFrame.pg_copy_from('users', engine)

    ``pg_copy_from`` supports many of the same parameters as
    ``read_sql`` and ``read_csv``.

    ``pg_copy_from`` also accepts the optimization parameter
    ``buffer_size``, which controls the maximum number of CSV-encoded
    results written by the database cursor to hold in memory prior to
    their being read into the ``DataFrame``. Depending on use-case,
    increasing this value may speed up the operation, at the cost of
    additional memory -- and vice-versa. ``buffer_size`` defaults to
    ``100``.

    In addition, the optimization parameter ``iter_size`` may be
    specified, which instructs ``pg_copy_from`` to use the named cursor
    feature of ``psycopg2``, (and so a server-size cursor), with the
    given *cursor* buffer ("iter") size. This parameter allows the user
    even lower-level control, over the number of database requests that
    are made, and the number of database results to hold in memory prior
    to their being read from the cursor.

    **Note**: This level of optimizational tuning, (especially
    ``iter_size``), *may not* produce a significant, measurable result
    for your task. For that matter, you might find the SQLAlchemy
    connection ``execution_option`` -- ``stream_results=True`` -- at
    least as useful. Consider performance profiling your task.

    """
    if isinstance(engine, str):
        raise TypeError("only SQLAlchemy engine supported not 'str'")

    pandas_sql = pandas.io.sql.SQLDatabase(engine)

    try:
        is_table_name = pandas_sql.has_table(sql)
    except Exception:
        # using generic exception to catch errors from sql drivers (GH24988)
        is_table_name = False

    if is_table_name:
        if columns:
            source = "{} ({})".format(
                sql,
                ', '.join('"{}"'.format(column) for column in columns),
            )
        else:
            source = sql
    elif columns:
        raise TypeError("'columns' supported only when copying from table")
    else:
        source = "({})".format(sql)

    if iter_size:
        cursor_name = _get_cursor_name('df_copy_from_')
    else:
        cursor_name = None

    with contextlib.closing(engine.raw_connection()) as conn:
        cursor = conn.cursor(name=cursor_name)

        if iter_size:
            cursor.itersize = iter_size

        writer = functools.partial(
            cursor.copy_expert,
            'COPY {source} TO STDOUT WITH CSV HEADER'.format(source=source),
        )

        with ohio.pipe_text(writer, buffer_size=buffer_size) as pipe:
            return pandas.read_csv(
                pipe,
                index_col=index_col,
                parse_dates=parse_dates,
                dtype=dtype,
                nrows=nrows,
            )


@pandas.api.extensions.register_dataframe_accessor('pg_copy_from')
@functools.wraps(data_frame_pg_copy_from)
def static_accessor_data_frame_pg_copy_from(*args, **kwargs):
    # Unlike pg_copy_to, this is *not* a DataFrame instance accessor;
    # static_accessor_data_frame_pg_copy_from is merely a registration
    # shim for the static/class method, (because Pandas does not provide
    # registration of these, and we'd prefer not to straight-up monkey-patch).

    if len(args) == 1 and isinstance(args[0], pandas.DataFrame):
        # accessed from instance
        raise TypeError("'pg_copy_from' constructs objects of type 'DataFrame': "
                        "df = DataFrame.pg_copy_from(...)")

    return data_frame_pg_copy_from(*args, **kwargs)
