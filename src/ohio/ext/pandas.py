"""
Extensions for Pandas
---------------------

This module extends ``pandas.DataFrame`` with methods ``pg_copy_to`` and
``pg_copy_from``.

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

**Note**: These extensions are intended for Pandas, and attempt to
``import pandas``. Pandas must be available (installed) in your
environment.

"""
import functools

import ohio
import pandas


@pandas.api.extensions.register_dataframe_accessor('pg_copy_to')
class DataFramePgCopyTo:
    """``pg_copy_to``: Copy ``DataFrame`` to database table via
    PostgreSQL ``COPY``.

    ``ohio.CsvTextIO`` enables the direct reading of ``DataFrame`` CSV
    into the "standard input" of the PostgreSQL ``COPY`` command, for
    quick, memory-efficient database persistence, (and without the
    needless involvement of the local file system).

    For example, given a SQLAlchemy ``connectable`` – either a database
    connection ``Engine`` or ``Connection`` – and a Pandas
    ``DataFrame``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('postgresql://')

        >>> df = pandas.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})

    We may simply invoke the ``DataFrame``'s Ohio extension method,
    ``pg_copy_to``::

        >>> df.pg_copy_to('users', engine)

    ``pg_copy_to`` supports all the same parameters as ``to_sql``,
    (excepting parameter ``method``).

    """

    def __init__(self, data_frame):
        self.data_frame = data_frame

    @functools.wraps(pandas.DataFrame.to_sql)
    def __call__(self, *args, **kwargs):
        self.data_frame.to_sql(
            *args,
            method=to_sql_method_pg_copy_to,
            **kwargs,
        )


def to_sql_method_pg_copy_to(table, engine, keys, data_iter):
    """Write pandas data to table via stream through PostgreSQL
    ``COPY``.

    This implements a pandas ``to_sql`` "method", utilizing
    ``ohio.CsvTextIO`` for performance stability.

    """
    columns = ', '.join('"{}"'.format(key) for key in keys)
    if table.schema:
        table_name = '{}.{}'.format(table.schema, table.name)
    else:
        table_name = table.name

    sql = 'COPY {table_name} ({columns}) FROM STDIN WITH CSV'.format(
        table_name=table_name,
        columns=columns,
    )

    with ohio.CsvTextIO(data_iter) as csv_buffer:
        with engine.connect() as conn:
            with conn.connection.cursor() as cursor:
                cursor.copy_expert(sql, csv_buffer)


def data_frame_pg_copy_from(sql, engine,
                            schema=None, index_col=None, parse_dates=False,
                            columns=None, dtype=None, nrows=None,
                            buffer_size=100):
    """``pg_copy_from``: Construct ``DataFrame`` from database table or
    query via PostgreSQL ``COPY``.

    ``ohio.PipeTextIO`` enables the direct, in-process "piping" of
    the PostgreSQL ``COPY`` command into Pandas ``read_csv``, for quick,
    memory-efficient construction of ``DataFrame`` from database, (and
    without the needless involvement of the local file system).

    For example, given a SQLAlchemy ``connectable`` – either a database
    connection ``Engine`` or ``Connection``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('postgresql://')

    We may simply invoke the ``DataFrame``'s Ohio extension method,
    ``pg_copy_from``::

        >>> df = DataFrame.pg_copy_from('users', engine)

    ``pg_copy_from`` supports many of the same parameters as
    ``read_sql`` and ``read_csv``.

    In addition, ``pg_copy_from`` accepts the optimization parameter
    ``buffer_size``, which controls the maximum number of CSV-encoded
    results written by the database cursor to hold in memory prior to
    their being read into the ``DataFrame``. Depending on use-case,
    increasing this value may speed up the operation, at the cost of
    additional memory -- and vice-versa. ``buffer_size`` defaults to
    ``100``.

    """
    pandas_sql = pandas.io.sql.SQLDatabase(engine)

    try:
        is_table_name = pandas_sql.has_table(sql, schema)
    except Exception:
        # using generic exception to catch errors from sql drivers (GH24988)
        is_table_name = False

    if is_table_name:
        table_name = '{}.{}'.format(schema, sql) if schema else sql

        if columns:
            source = "{} ({})".format(
                table_name,
                ', '.join('"{}"'.format(column) for column in columns),
            )
        else:
            source = table_name
    elif columns or schema:
        if ' ' in sql:
            hint = ""
        else:
            if schema:
                schema_description = 'schema {!r}'.format(schema)
            else:
                schema_description = 'default schema'

            hint = " (hint: no such table {!r} found under {})".format(
                sql, schema_description)

        raise TypeError("keyword arguments 'columns' and 'schema' supported "
                        "only when copying from table" + hint)
    else:
        source = "({})".format(sql)

    with engine.connect() as conn:
        with conn.connection.cursor() as cursor:

            writer = functools.partial(
                cursor.copy_expert,
                'COPY {source} TO STDOUT WITH CSV HEADER'.format(
                    source=source),
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
        raise AttributeError("'pg_copy_from' constructs objects of type "
                             "'DataFrame': df = DataFrame.pg_copy_from(...)")

    return data_frame_pg_copy_from(*args, **kwargs)
