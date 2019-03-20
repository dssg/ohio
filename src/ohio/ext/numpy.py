import collections
import contextlib
import csv
import functools

import numpy
import ohio


# TODO: test
def pg_copy_to_table(arr, name, engine, columns=None, fmt=None):
    target = _sql_table_columns(name, columns)
    sql = 'COPY {target} FROM STDIN WITH CSV'.format(target=target)

    kwargs = {'fmt': fmt} if fmt is not None else {}
    writer = functools.partial(_write_csv, arr, **kwargs)

    with ohio.PipeTextIO(writer) as pipe, \
            contextlib.closing(engine.raw_connection()) as conn:
        cursor = conn.cursor()
        cursor.copy_expert(sql, pipe)


def _write_csv(arr, buffer, **kwargs):
    numpy.savetxt(buffer, arr, delimiter=',', **kwargs)


def pg_copy_from_query(query, engine, dtype):
    return _pg_copy_from("({})".format(query), engine, dtype)


def pg_copy_from_table(name, engine, dtype, columns=None):
    source = _sql_table_columns(name, columns)
    return _pg_copy_from(source, engine, dtype)


class NpCsvReader:

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


def _pg_copy_from(source, engine, dtype):
    if isinstance(engine, str):
        raise TypeError("only SQLAlchemy engine supported not 'str'")

    with contextlib.closing(engine.raw_connection()) as conn:
        cursor = conn.cursor()

        writer = functools.partial(
            cursor.copy_expert,
            'COPY {source} TO STDOUT WITH CSV'.format(source=source),
        )

        with ohio.PipeTextIO(writer) as pipe:
            reader = NpCsvReader(pipe, squash=True)
            arr = numpy.fromiter(reader, dtype=dtype)

    arr.shape = reader.shape
    return arr


def _sql_table_columns(table, columns=None):
    sql = str(table)

    if columns:
        sql += " ({})".format(
            ', '.join('"{}"'.format(column) for column in columns),
        )

    return sql
