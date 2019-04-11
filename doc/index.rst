====
Ohio
====


.. automodule:: ohio

    For higher-level examples of what Ohio can do for you, see `Extensions`_.


.. contents::


.. automodule:: ohio.csvio

.. autofunction:: ohio.csv_text

.. autoclass:: ohio.CsvWriterTextIO

.. autoclass:: ohio.CsvDictWriterTextIO


.. automodule:: ohio.iterio

.. autoclass:: ohio.IteratorTextIO


.. automodule:: ohio.pipeio

.. autofunction:: ohio.pipe_text


.. automodule:: ohio.baseio

.. autoclass:: ohio.StreamTextIOBase

.. autoexception:: ohio.IOClosed


.. _Extensions:

.. automodule:: ohio.ext

    .. automodule:: ohio.ext.pandas

        .. autoclass:: ohio.ext.pandas.DataFramePgCopyTo

        .. autofunction:: ohio.ext.pandas.to_sql_method_pg_copy_to

        .. autofunction:: ohio.ext.pandas.data_frame_pg_copy_from

    Benchmarking
    ~~~~~~~~~~~~

    Ohio extensions for pandas were benchmarked to test their speed and
    memory-efficiency relative both to pandas built-in functionality and
    to custom implementations which do not utilize Ohio.

    Interfaces and syntactical niceties aside, Ohio generally features
    memory stability. Its tools enable pipelines which *may* also
    improve speed.

    In the below benchmark, Ohio extensions ``pg_copy_from`` &
    ``pg_copy_to`` reduced memory consumption by 84% & 61%, and
    completed in 39% & 89% less time, relative to pandas built-ins
    ``read_sql`` & ``to_sql``, (respectively).

    Compared to purpose-built extensions – which utilized PostgreSQL
    ``COPY``, but using ``io.StringIO`` in place of ``ohio.PipeTextIO``
    – ``pg_copy_from`` & ``pg_copy_to`` still reduced memory consumption
    by 60% & 33%, respectively. ``pg_copy_from`` also completed in 16%
    less time than the ``io.StringIO`` version. ``pg_copy_to`` took on
    average 7% more time to complete than the ``io.StringIO`` version.
    (Speed improvements – which do not diminish Ohio's memory
    efficiency – have been identified as a target for future work.)

    The benchmarks plotted below were produced from averages and
    standard deviations over 3 randomized trials per target. Input data
    consisted of 896,677 rows across 83 columns: 1 of these of type
    timestamp, 51 integers and 31 floats. The benchmarking package,
    ``prof``, is preserved in `Ohio's repository
    <https://github.com/dssg/ohio>`_.

    .. Rather than relative links to assets in the repository, we use
       absolute links to githubusercontent.com, to ensure document
       presents the same outside of Github, (e.g. on PyPI).

    .. Also note the query string ?sanitize=true
       This is necessary to ensure that SVG is returned with the image content type.

    .. image:: https://raw.githubusercontent.com/dssg/ohio/0.2.0/doc/img/profile-copy-from-database-to-datafram-1554345457.svg?sanitize=true

    ohio_pg_copy_from_X
        ``pg_copy_from(buffer_size=X)``

        A PostgreSQL database-connected cursor writes the results of
        ``COPY`` to a ``PipeTextIO``, from which pandas constructs a
        ``DataFrame``.

    pandas_read_sql
        ``pandas.read_sql()``

        Pandas constructs a ``DataFrame`` from a given database query.

    pandas_read_sql_chunks_100
        ``pandas.read_sql(chunksize=100)``

        Pandas is instructed to generate ``DataFrame`` slices of the
        database query result, and these slices are concatenated into a
        single frame, with: ``pandas.concat(chunks, copy=False)``.

    pandas_read_csv_stringio
        ``pandas.read_csv(StringIO())``

        A PostgreSQL database-connected cursor writes the results of
        ``COPY`` to a ``StringIO``, from which pandas constructs a
        ``DataFrame``.

    .. image:: https://raw.githubusercontent.com/dssg/ohio/0.2.0/doc/img/profile-copy-from-dataframe-to-databas-1554320666.svg?sanitize=true

    ohio_pg_copy_to_X
        ``pg_copy_to(buffer_size=X)``

        ``DataFrame`` data are written and encoded through a
        ``PipeTextIO``, and read by a PostgreSQL database-connected
        cursor's ``COPY`` command.

    pandas_to_sql
        ``pandas.DataFrame.to_sql()``

        Pandas inserts ``DataFrame`` data into the database row by row.

    pandas_to_sql_multi_X
        ``pandas.DataFrame.to_sql(method='multi', chunksize=X)``

        Pandas inserts ``DataFrame`` data into the database in chunks of
        rows.

    copy_stringio_to_db
        ``DataFrame`` data are written and encoded to a ``StringIO``,
        and then read by a PostgreSQL database-connected cursor's
        ``COPY`` command.

.. only:: not noindex

    Indices and tables
    ==================

    * :ref:`genindex`
    * :ref:`modindex`
    * :ref:`search`
