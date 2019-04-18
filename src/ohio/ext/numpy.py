"""
Extensions for NumPy
--------------------

This module enables writing NumPy array data to database and populating
arrays from database via PostgreSQL ``COPY``. The operation is ensured,
by Ohio, to be memory-efficient.

**Note**: This integration is intended for NumPy, and attempts to
``import numpy``. NumPy must be available (installed) in your
environment.

"""
import collections
import csv
import functools

import numpy

import ohio


# Externals #

def pg_copy_to_table(arr, table_name, connectable, columns=None, fmt=None):
    """Copy ``array`` to database table via PostgreSQL ``COPY``.

    ``ohio.PipeTextIO`` enables the direct, in-process "piping" of
    ``array`` CSV into the "standard input" of the PostgreSQL
    ``COPY`` command, for quick, memory-efficient database persistence,
    (and without the needless involvement of the local file system).

    For example, given a SQLAlchemy ``connectable`` – either a database
    connection ``Engine`` or ``Connection`` – and a NumPy ``array``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('postgresql://')

        >>> arr = numpy.array([1.000102487, 5.982, 2.901, 103.929])

    We may persist this data to an existing table – *e.g.* "data"::

        >>> pg_copy_to_table(arr, 'data', engine, columns=['value'])

    ``pg_copy_to_table`` utilizes ``numpy.savetxt`` and supports its
    ``fmt`` parameter.

    """
    target = _sql_table_columns(table_name, columns)
    sql = 'COPY {target} FROM STDIN WITH CSV'.format(target=target)

    savetxt_kwargs = {'fmt': fmt} if fmt is not None else {}

    # NOTE: It would be nice to use CsvTextIO in place of PipeTextIO here,
    # NOTE: and this might provide a minor speed boost, (as with pg_copy_to).
    #
    # NOTE: However, while the "meat" of savetxt isn't much, it would also be
    # NOTE: nice to avoid reimplementing its formatting (fmt) logic, and its
    # NOTE: handling of array dimensionality -- (at least for now).
    #
    # NOTE: That said, there's probably not much to it, and the fmt logic is
    # NOTE: arguably far less important when the "txt" being "saved" is only an
    # NOTE: intermediary between the source (array) and the target (database).
    # NOTE: The fmt is relatively inconsequential so long as the database
    # NOTE: schema enforces the proper typing and precision.
    #
    # NOTE: Rather, fmt matters most to size of the generated CSV; but, at that
    # NOTE: level, it's just an optimization parameter. (We'd want to keep
    # NOTE: floats long by default, to avoid losing precision, but allow user
    # NOTE: to shorten as they like via custom fmt, for smaller CSV payloads.)
    with ohio.pipe_text(numpy.savetxt,
                        arr,
                        delimiter=',',
                        **savetxt_kwargs) as pipe, \
            connectable.begin() as tx:
        # connectable is either an Engine or a Connection,
        # and as such tx is either a new Connection or the Transaction
        conn = tx if hasattr(tx, 'execute') else connectable
        cursor = conn.connection.cursor()
        cursor.copy_expert(sql, pipe)


def pg_copy_from_query(query, connectable, dtype):
    """Construct ``array`` from database ``query`` via PostgreSQL
    ``COPY``.

    ``ohio.PipeTextIO`` enables the in-process "piping" of the
    PostgreSQL ``COPY`` command into NumPy's ``fromiter``, for quick,
    memory-efficient construction of ``array`` from database, (and
    without the needless involvement of the local file system).

    For example, given a SQLAlchemy ``connectable`` – either a database
    connection ``Engine`` or ``Connection``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('postgresql://')

    We may construct a NumPy ``array`` from a given query::

        >>> arr = pg_copy_from_query(
        ...     'select value0, value1, value3 from data',
        ...     engine,
        ...     float,
        ... )

    """
    return _pg_copy_from("({})".format(query), connectable, dtype)


def pg_copy_from_table(table_name, connectable, dtype, columns=None):
    """Construct ``array`` from database table via PostgreSQL ``COPY``.

    ``ohio.PipeTextIO`` enables the in-process "piping" of the
    PostgreSQL ``COPY`` command into NumPy's ``fromiter``, for quick,
    memory-efficient construction of ``array`` from database, (and
    without the needless involvement of the local file system).

    For example, given a SQLAlchemy ``connectable`` – either a database
    connection ``Engine`` or ``Connection``::

        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('postgresql://')

    We may construct a NumPy ``array`` from the contents of a specified
    table::

        >>> arr = pg_copy_from_table(
        ...     'data',
        ...     engine,
        ...     float,
        ... )

    """
    source = _sql_table_columns(table_name, columns)
    return _pg_copy_from(source, connectable, dtype)


# Internals #

class NpCsvReader:
    """CSV decoder, wrapping ``csv.reader``, which records the ``shape``
    of the data it decodes.

    When initialized with the ``squash`` flag, iteration of
    ``NpCsvReader`` returns data cells one-by-one, rather than in rows.
    (This allows its use to populate NumPy ``array`` via ``fromiter``.)

    """
    def __init__(self, encoded, squash=False):
        self.reader = csv.reader(encoded)
        self.row_count = 0
        self.row_size = None
        self.remainder = collections.deque() if squash else None

    @property
    def shape(self):
        return (self.row_count, self.row_size)

    def __iter__(self):
        return self

    def __next__(self):
        if self.remainder:
            return self.remainder.popleft()

        decoded = next(self.reader)

        if self.row_count == 0:
            self.row_size = len(decoded)

        self.row_count += 1

        if self.remainder is None:
            return decoded

        self.remainder.extend(decoded)
        return self.__next__()


def _pg_copy_from(source, connectable, dtype):
    """Construct ``array`` from database via PostgreSQL ``COPY``."""
    with connectable.connect() as conn:
        # connectable is either an Engine or a Connection.
        # *Iff* they gave us a Connection, we don't want to close it
        # for them; so, in either case we want to call connect():
        # * either we'll create a new Connection from an Engine,
        # * or we'll create a *branched* Connection (which we can close).
        cursor = conn.connection.cursor()

        writer = functools.partial(
            cursor.copy_expert,
            'COPY {source} TO STDOUT WITH CSV'.format(source=source),
        )

        with ohio.pipe_text(writer) as pipe:
            reader = NpCsvReader(pipe, squash=True)
            arr = numpy.fromiter(reader, dtype=dtype)

    arr.shape = reader.shape
    return arr


def _sql_table_columns(table, columns=None):
    """Encode given table and optional columns for use in SQL."""
    sql = str(table)

    if columns:
        sql += " ({})".format(
            ', '.join('"{}"'.format(column) for column in columns),
        )

    return sql
