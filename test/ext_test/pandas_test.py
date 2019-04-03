import operator
from datetime import datetime

import numpy
import pandas
import pytest
import sqlalchemy
import testing.postgresql

import ohio.ext.pandas  # noqa


class TestPandasExt:

    names = ('Alice', 'Bob', 'Conner', 'Denise')

    @staticmethod
    def table_exists(table, engine):
        result = engine.execute(f"select to_regclass('{table}')").scalar()
        return bool(result)

    @pytest.fixture
    def engine(self):
        with testing.postgresql.Postgresql() as postgresql:
            engine = sqlalchemy.create_engine(postgresql.url())
            yield engine
            engine.dispose()

    @pytest.fixture(name='df')
    def names_df(self):
        return pandas.DataFrame({'name': self.names})

    @pytest.mark.parametrize('method', ('pg_copy_to', 'pg_copy_from'))
    def test_dataframe_class_access(self, method):
        "pg_copy_* available on DataFrame constructor"
        assert hasattr(pandas.DataFrame, method)

    def test_dataframe_object_access(self, df):
        "only pg_copy_to available on DataFrame instance"
        assert hasattr(df, 'pg_copy_to')
        assert not hasattr(df, 'pg_copy_from')

    def test_dataframe_object_dir(self, df):
        "DataFrame instance __dir__ works and only includes pg_copy_to"
        members = dir(df)
        assert 'pg_copy_to' in members
        assert 'pg_copy_from' not in members

    def test_pg_copy_to(self, engine, df):
        assert not self.table_exists('users', engine)

        df.pg_copy_to('users', engine)

        assert self.table_exists('users', engine)

        results = engine.execute("select * from users")
        assert results.keys() == ['index', 'name']
        assert results.fetchall() == list(enumerate(self.names))

    def test_pg_copy_from(self, engine):
        users = (
            ('Alice', datetime(2019, 1, 2, 13, 0, 0), 302.1),
            ('Bob', datetime(2018, 10, 20, 8, 7, 10), 2.4),
            ('Conner', datetime(2019, 2, 20, 11, 53, 0), 30.9),
            ('Denise', datetime(2019, 3, 2, 9, 26, 22), 3005.102),
        )

        with engine.connect() as conn:
            conn.execute(
                "create table users ("
                "    id serial,"
                "    name varchar,"
                "    last_login timestamp,"
                "    tetris_high_score double precision"
                ")"
            )

            for user in users:
                conn.execute(
                    "insert into users "
                    "   (name, last_login, tetris_high_score) "
                    "values (%s, %s, %s)",
                    user
                )

        df = pandas.DataFrame.pg_copy_from(
            'users',
            engine,
            index_col='id',
            parse_dates=['last_login'],
        )

        assert df.index.name == 'id'
        assert df.index.dtype == 'int64'
        assert df.index.tolist() == [1, 2, 3, 4]

        assert df.columns.tolist() == ['name', 'last_login', 'tetris_high_score']
        assert df.dtypes.to_dict() == {
            'name': 'object',
            'last_login': 'datetime64[ns]',
            'tetris_high_score': 'float64',
        }

        df_01_values = df[['name', 'last_login']].values
        expected_01_values = list(map(operator.itemgetter(0, 1), users))
        assert (df_01_values == expected_01_values).all()

        df_2_values = df['tetris_high_score'].values
        expected_2_values = list(map(operator.itemgetter(2), users))
        assert numpy.isclose(df_2_values, expected_2_values).all()
