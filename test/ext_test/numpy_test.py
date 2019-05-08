import functools

import numpy as np
import pytest
import sqlalchemy
import testing.postgresql

from ohio.ext import numpy as op


# FIXME: use shared once that's merged
@pytest.fixture
def engine():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sqlalchemy.create_engine(postgresql.url())
        yield engine
        engine.dispose()


@pytest.fixture(name='test_engine')
def setup_engine(engine):
    engine.execute(
        "create table data ("
        "    id serial,"
        "    value0 double precision,"
        "    value1 double precision,"
        "    value2 double precision"
        ")"
    )
    return engine


def get_connectable(test_engine, use_conn):
    return test_engine.connect() if use_conn else test_engine


@pytest.mark.parametrize('use_conn', (True, False))
class TestNumpyExtPgCopyTo:

    def test_pg_copy_to_table_1d(self, test_engine, use_conn):
        arr = np.array([1.000102487, 5.982, 2.901, 103.929])

        op.pg_copy_to_table(
            arr,
            'data',
            get_connectable(test_engine, use_conn),
            columns=['value0'],
        )

        persisted = test_engine.execute('select id, value0 from data').fetchall()
        assert persisted == list(enumerate(arr, 1))

    def test_pg_copy_to_table_2d(self, test_engine, use_conn):
        arr = np.array([
            [1.000102487, 5.982, 2.901],
            [103.929, 0.000102, 7.9],
            [29.103, 8.12, 2.1000002],
        ])

        op.pg_copy_to_table(
            arr,
            'data',
            get_connectable(test_engine, use_conn),
            columns=['value0', 'value1', 'value2'],
        )

        persisted = test_engine.execute('select * from data').fetchall()
        assert persisted == [
            (index,) + tuple(vals) for (index, vals) in enumerate(arr, 1)
        ]

    def test_pg_copy_to_table_fmt(self, test_engine, use_conn):
        arr = np.array([
            [1.000102487, 5.982, 2.901],
            [103.929, 0.000102, 7.9],
            [29.103, 8.12, 2.1000002],
        ])

        op.pg_copy_to_table(
            arr,
            'data',
            get_connectable(test_engine, use_conn),
            columns=['value0', 'value1', 'value2'],
            fmt='%1.3f',
        )

        persisted = test_engine.execute('select * from data').fetchall()

        rounder = functools.partial(round, ndigits=3)
        expected = [(index,) + tuple(map(rounder, vals))
                    for (index, vals) in enumerate(arr, 1)]

        assert persisted == expected


@pytest.mark.parametrize('use_conn', (True, False))
class TestNumpyExtPgCopyFrom:

    data = (
        (1.000102487, 5.982, 2.901),
        (103.929, 0.000102, 7.9),
        (29.103, 8.12, 2.1000002),
    )

    @pytest.fixture(name='test_engine')
    def setup_engine(self, test_engine):
        with test_engine.connect() as conn:
            for vals in self.data:
                conn.execute(
                    'insert into data (value0, value1, value2) '
                    'values (%s, %s, %s)',
                    vals
                )

        return test_engine

    def test_pg_copy_from_query(self, test_engine, use_conn):
        arr = op.pg_copy_from_query(
            'select * from data',
            get_connectable(test_engine, use_conn),
            float,
        )
        assert arr.shape == (3, 4)
        assert arr.tolist() == [
            [index] + list(vals) for (index, vals) in enumerate(self.data, 1)
        ]

    def test_pg_copy_from_table(self, test_engine, use_conn):
        arr = op.pg_copy_from_table(
            'data',
            get_connectable(test_engine, use_conn),
            float,
        )
        assert arr.shape == (3, 4)
        assert arr.tolist() == [
            [index] + list(vals) for (index, vals) in enumerate(self.data, 1)
        ]

    def test_pg_copy_from_table_columns(self, test_engine, use_conn):
        arr = op.pg_copy_from_table(
            'data',
            get_connectable(test_engine, use_conn),
            float,
            ['value1', 'value2'],
        )
        assert arr.shape == (3, 2)
        assert arr.tolist() == [list(vals[1:]) for vals in self.data]
