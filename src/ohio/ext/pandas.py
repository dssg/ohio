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

import ohio
import pandas
import pandas.io.sql


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

    """
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def __call__(self, *args, **kwargs):
        self.data_frame.to_sql(
            *args,
            method=data_frame_pg_copy_to,
            **kwargs,
        )

        # previously (but this couldn't easily support all of to_sql's features):
        # with ohio.PipeTextIO(self.data_frame.to_csv) as pipe, \
        #         contextlib.closing(engine.raw_connection()) as conn:
        #     cursor = conn.cursor()
        #
        #     cursor.copy_expert(
        #         'COPY {name} FROM STDIN WITH CSV HEADER'.format(name=name),
        #         pipe,
        #     )


def _write_csv(rows, buffer):
    writer = csv.writer(buffer)
    writer.writerows(rows)


def data_frame_pg_copy_to(table, conn, keys, data_iter):
    columns = ', '.join('"{}"'.format(key) for key in keys)
    if table.schema:
        table_name = '{}.{}'.format(table.schema, table.name)
    else:
        table_name = table.name

    sql = 'COPY {table_name} ({columns}) FROM STDIN WITH CSV'.format(
        table_name=table_name,
        columns=columns,
    )

    # Note: this could use a csv stream rather than csv.writer
    writer = functools.partial(_write_csv, data_iter)

    with ohio.PipeTextIO(writer) as pipe, \
            conn.connection.cursor() as cursor:
        cursor.copy_expert(sql, pipe)


@pandas.api.extensions.register_dataframe_accessor('pg_copy_from')
class DataFramePgCopyFrom:
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

    """
    # Unlike pg_copy_to, this is *not* a DataFrame instance accessor;
    # DataFramePgCopyFrom is merely a registration shim for the
    # static/class method, (because Pandas does not provide registration
    # of these, and we'd prefer not to straight-up monkey-patch).

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], pandas.DataFrame):
            # accessed from instance
            raise TypeError("'pg_copy_from' constructs objects of type 'DataFrame': "
                            "df = DataFrame.pg_copy_from(...)")

        return data_frame_pg_copy_from(*args, **kwargs)


def data_frame_pg_copy_from(sql, engine,
                            index_col=None, parse_dates=False, columns=None,
                            dtype=None, nrows=None):
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

    with contextlib.closing(engine.raw_connection()) as conn:
        cursor = conn.cursor()

        writer = functools.partial(
            cursor.copy_expert,
            'COPY {source} TO STDOUT WITH CSV HEADER'.format(source=source),
        )

        with ohio.PipeTextIO(writer) as pipe:
            return pandas.read_csv(
                pipe,
                index_col=index_col,
                parse_dates=parse_dates,
                dtype=dtype,
                nrows=nrows,
            )
