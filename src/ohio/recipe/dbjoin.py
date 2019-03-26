"""
dbjoin
------

Join the "COPY" results of arbitrary database queries in Python, without
unnecessary memory overhead.

This is largely useful to work around databases' per-query column limit.

"""
import contextlib
import functools

import ohio


JOIN_QUERIES_SEP = ','
JOIN_QUERIES_END = '\n'


def db_join_queries(copy_sqls, engine, sep=JOIN_QUERIES_SEP, end=JOIN_QUERIES_END):
    """Join the text-encoded result streams of an arbitrary number of
    database queries to work around the database's per-query column
    limit.

    **NOTE**: This is a lower-level function, currently utilized only by
    ``pg_join_queries``. As such, it expects ``copy_sqls`` – statements
    which appropriately wrap user queries for ``COPY`` – and, it assumes
    that the database driver exposes method ``copy_expert`` via its
    cursor. This function is so factored in support of future
    implementation against additional database systems, and in order to
    best showcase this Ohio recipe.

    """
    with contextlib.ExitStack() as stack:
        raw_conns = iter(engine.raw_connection, None)
        connections = (stack.enter_context(contextlib.closing(conn))
                       for conn in raw_conns)
        cursors = (conn.cursor() for conn in connections)

        writers = (functools.partial(cursor.copy_expert, copy_sql)
                   for (cursor, copy_sql) in zip(cursors, copy_sqls))
        pipes = (stack.enter_context(ohio.pipe_text(writer)) for writer in writers)

        for join in zip(*pipes):
            yield sep.join(line.rstrip('\r\n') for line in join) + end


def pg_join_queries(queries, engine,
                    sep=JOIN_QUERIES_SEP,
                    end=JOIN_QUERIES_END,
                    copy_options=('CSV', 'HEADER')):
    r"""Join the text-encoded result streams of an arbitrary number of
    PostgreSQL database queries to work around the database's per-query
    column limit.

    Query results are read via PostgreSQL ``COPY``, streamed through
    ``PipeTextIO``, and joined line-by-line into a singular stream.

    For example, given a set of database queries whose results cannot be
    combined into a single PostgreSQL query, we might join these
    queries' results and write these results to a file-like object::

        >>> queries = [
        ...     'SELECT a, b, c FROM a_table',
        ...     ...
        ... ]

        >>> with open('results.csv', 'w', newline='') as fdesc:
        ...     for line in pg_join_queries(queries, engine):
        ...         fdesc.write(line)

    Or, we might read these results into a single Pandas DataFrame::

        >>> csv_lines = pg_join_queries(queries, engine)
        >>> csv_buffer = ohio.IteratorTextIO(csv_lines)
        >>> df = pandas.read_csv(csv_buffer)

    By default, ``pg_join_queries`` requests CSV-encoded results, with
    an initial header line indicating the result columns. These options,
    which are sent directly to the PostgreSQL ``COPY`` command, may be
    controlled via ``copy_options``. For example, to omit the CSV
    header::

        >>> pg_join_queries(queries, engine, copy_options=['CSV'])

    Or, to request PostgreSQL's tab-delimited text format via the syntax
    of PostgreSQL v9.0+::

        >>> pg_join_queries(
        ...     queries,
        ...     engine,
        ...     sep='\t',
        ...     copy_options={'FORMAT': 'TEXT'},
        ... )

    In the above example, we've instructed PostgreSQL to use its
    ``text`` results encoder, (and we've omitted the instruction to
    include a header).

    **NOTE**: In the last example, we also explicitly specified the
    separator used in the results' encoding. This is not passed to the
    database; rather, it is necessary for ``pg_join_queries`` to
    properly join queries' results.

    """
    copy_template = "COPY ({query}) TO STDOUT"
    if copy_options:
        if hasattr(copy_options, 'items'):
            copy_options_v9 = ("{} {}".format(key, value)
                               for (key, value) in copy_options.items())
            copy_template += " WITH ({})".format(", ".join(copy_options_v9))
        else:
            copy_template += " WITH " + " ".join(copy_options)

    copy_sqls = (copy_template.format(query=query) for query in queries)
    return db_join_queries(copy_sqls, engine, sep, end)
