import operator
from datetime import datetime

import numpy
import pandas
import pytest

import ohio
from ohio.recipe import dbjoin


class TestDbJoinRecipe:

    users = (
        ('Alice', datetime(2019, 1, 2, 13, 0, 0), 302.1),
        ('Bob', datetime(2018, 10, 20, 8, 7, 10), 2.4),
        ('Conner', datetime(2019, 2, 20, 11, 53, 0), 30.9),
        ('Denise', datetime(2019, 3, 2, 9, 26, 22), 3005.102),
    )

    @pytest.fixture(name='test_engine')
    def setup_database(self, engine):
        with engine.connect() as conn:
            conn.execute(
                "create table users ("
                "    id serial,"
                "    name varchar,"
                "    last_login timestamp,"
                "    tetris_high_score double precision"
                ")"
            )

            for user in self.users:
                conn.execute(
                    "insert into users "
                    "   (name, last_login, tetris_high_score) "
                    "values (%s, %s, %s)",
                    user
                )

        return engine

    def test_pg_join_queries(self, test_engine):
        joined_results = dbjoin.pg_join_queries(
            [
                'select id, last_login from users',
                'select name, tetris_high_score from users',
            ],
            test_engine,
        )
        assert list(joined_results) == [
            'id,last_login,name,tetris_high_score\n',
            '1,2019-01-02 13:00:00,Alice,302.1\n',
            '2,2018-10-20 08:07:10,Bob,2.4\n',
            '3,2019-02-20 11:53:00,Conner,30.9\n',
            '4,2019-03-02 09:26:22,Denise,3005.102\n',
        ]

    def test_pg_join_queries_noheader(self, test_engine):
        joined_results = dbjoin.pg_join_queries(
            [
                'select id, last_login from users',
                'select name, tetris_high_score from users',
            ],
            test_engine,
            copy_options=['CSV'],
        )
        assert list(joined_results) == [
            '1,2019-01-02 13:00:00,Alice,302.1\n',
            '2,2018-10-20 08:07:10,Bob,2.4\n',
            '3,2019-02-20 11:53:00,Conner,30.9\n',
            '4,2019-03-02 09:26:22,Denise,3005.102\n',
        ]

    def test_pg_join_queries_text(self, test_engine):
        joined_results = dbjoin.pg_join_queries(
            [
                'select id, last_login from users',
                'select name, tetris_high_score from users',
            ],
            test_engine,
            sep='\t',
            copy_options={'FORMAT': 'TEXT'},
        )
        assert list(joined_results) == [
            '1\t2019-01-02 13:00:00\tAlice\t302.1\n',
            '2\t2018-10-20 08:07:10\tBob\t2.4\n',
            '3\t2019-02-20 11:53:00\tConner\t30.9\n',
            '4\t2019-03-02 09:26:22\tDenise\t3005.102\n',
        ]

    def test_pg_join_queries_dataframe(self, test_engine):
        joined_results = dbjoin.pg_join_queries(
            [
                'select id, last_login from users',
                'select name, tetris_high_score from users',
            ],
            test_engine,
        )
        csv_buffer = ohio.IteratorTextIO(joined_results)
        df = pandas.read_csv(
            csv_buffer,
            index_col='id',
            parse_dates=['last_login'],
        )

        assert df.index.tolist() == [1, 2, 3, 4]
        assert df.columns.tolist() == ['last_login', 'name', 'tetris_high_score']

        expected_01_values = list(map(operator.itemgetter(1, 0), self.users))
        expected_2_values = list(map(operator.itemgetter(2), self.users))

        assert (df[['last_login', 'name']].values == expected_01_values).all()
        assert numpy.isclose(df['tetris_high_score'].values, expected_2_values).all()
