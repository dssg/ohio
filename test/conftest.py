import pytest
import sqlalchemy
import testing.postgresql


@pytest.fixture(name='engine', scope='function')
def pg_engine():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sqlalchemy.create_engine(postgresql.url())
        yield engine
        engine.dispose()
