import contextlib
import io

import prof.tool
from prof.tool import (
    countcheck,
    create_table,
    loadconfig,
    loaddb,
    loadframe,
    mprof,
    time,
)


profiler = prof.tool.profiler('copy from dataframe to database')


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def pandas_to_sql(config, df, engine):
    """pandas.DataFrame.to_sql"""
    df.to_sql(config.table_name, engine)


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def pandas_to_sql_multi_100(config, df, engine):
    """pandas.DataFrame.to_sql(chunksize=100, method='multi')"""
    df.to_sql(config.table_name, engine, chunksize=100, method='multi')


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def pandas_to_sql_multi_1000(config, df, engine):
    """pandas.DataFrame.to_sql(chunksize=1_000, method='multi')"""
    df.to_sql(config.table_name, engine, chunksize=1_000, method='multi')


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def copy_stringio_to_db(config, df, engine):
    """DataFrame → StringIO → COPY"""
    with config.data_path.open() as fd:
        header = next(fd)

    create_table(header, engine)

    buffer = io.StringIO()
    df.to_csv(buffer)
    buffer.seek(0)

    with contextlib.closing(engine.raw_connection()) as conn:
        cursor = conn.cursor()
        cursor.copy_expert(
            f'copy {config.table_name} from stdin with csv header',
            buffer,
        )
        conn.commit()


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def ohio_pg_copy_to_1(config, df, engine):
    """pg_copy_to(buffer_size=1) {DataFrame → PipeTextIO → COPY}"""
    df.pg_copy_to(config.table_name, engine, buffer_size=1)


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def ohio_pg_copy_to_10(config, df, engine):
    """pg_copy_to(buffer_size=10) {DataFrame → PipeTextIO → COPY}"""
    df.pg_copy_to(config.table_name, engine, buffer_size=10)


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def ohio_pg_copy_to_100(config, df, engine):
    """pg_copy_to(buffer_size=100) {DataFrame → PipeTextIO → COPY}"""
    df.pg_copy_to(config.table_name, engine, buffer_size=100)


@profiler
@loaddb
@loadframe
@loadconfig
@countcheck
@mprof
@time
def ohio_pg_copy_to_1000(config, df, engine):
    """pg_copy_to(buffer_size=1000) {DataFrame → PipeTextIO → COPY}"""
    df.pg_copy_to(config.table_name, engine, buffer_size=1000)
