
Ohio
****

Oh! IO: The I/O tools that ``io`` doesn’t want you to have.

Ohio provides the missing links between Python’s built-in I/O
primitives, to help ensure the efficiency, clarity and elegance of
your code.

For higher-level examples of what Ohio can do for you, see
`Extensions`_ and `Recipes`_.


Contents
^^^^^^^^

* `Ohio`_

   * `Installation`_

   * `Modules`_

      * `csvio`_

      * `iterio`_

      * `pipeio`_

      * `baseio`_

      * `Extensions`_

         * `Extensions for NumPy`_

         * `Extensions for Pandas`_

         * `Benchmarking`_

      * `Recipes`_

         * `dbjoin`_


Installation
============

Ohio is a distributed library with support for Python v3. It is
available from `pypi.org <https://pypi.org/project/ohio/>`_:

::

   $ pip install ohio


Modules
=======


csvio
-----

Flexibly encode data to CSV format.

**ohio.encode_csv(rows, *writer_args, writer=<built-in function
writer>, write_header=False, **writer_kwargs)**

   Encode the specified iterable of ``rows`` into CSV text.

   Data is encoded to an in-memory ``str``, (rather than to the file
   system), via an internally-managed ``io.StringIO``, (newly
   constructed for every invocation of ``encode_csv``).

   For example:

   ::

      >>> data = [
      ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
      ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
      ... ]

      >>> encoded_csv = encode_csv(data)

      >>> encoded_csv[:80]
      '1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n1/2/09 4:53,Product1,1200,Visa,Be'

      >>> encoded_csv.splitlines(keepends=True)
      ['1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n',
       '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

   By default, ``rows`` are encoded by built-in ``csv.writer``. You
   may specify an alternate ``writer``, and provide construction
   arguments:

   ::

      >>> header = ('Transaction_date', 'Product', 'Price', 'Payment_Type', 'Name')

      >>> data = [
      ...     {'Transaction_date': '1/2/09 6:17',
      ...      'Product': 'Product1',
      ...      'Price': '1200',
      ...      'Payment_Type': 'Mastercard',
      ...      'Name': 'carolina'},
      ...     {'Transaction_date': '1/2/09 4:53',
      ...      'Product': 'Product1',
      ...      'Price': '1200',
      ...      'Payment_Type': 'Visa',
      ...      'Name': 'Betina'},
      ... ]

      >>> encoded_csv = encode_csv(data, writer=csv.DictWriter, fieldnames=header)

      >>> encoded_csv.splitlines(keepends=True)
      ['1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n',
       '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

   And, for such writers featuring the method ``writeheader``, you may
   instruct ``encode_csv`` to invoke this, prior to writing ``rows``:

   ::

      >>> encoded_csv = encode_csv(
      ...     data,
      ...     writer=csv.DictWriter,
      ...     fieldnames=header,
      ...     write_header=True,
      ... )

      >>> encoded_csv.splitlines(keepends=True)
      ['Transaction_date,Product,Price,Payment_Type,Name\r\n',
       '1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n',
       '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

**class ohio.CsvTextIO(rows, *writer_args, write_header=False,
chunk_size=10, **writer_kwargs)**

   Readable file-like interface encoding specified data as CSV.

   Rows of input data are only consumed and encoded as needed, as
   ``CsvTextIO`` is read.

   Rather than write to the file system, an internal ``io.StringIO``
   buffer is used to store output temporarily, until it is read. (Also
   unlike ``ohio.encode_csv``, this buffer is reused across read/write
   cycles.)

   For example, we might encode the following data as CSV:

   ::

      >>> data = [
      ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
      ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
      ... ]

      >>> csv_buffer = CsvTextIO(data)

   Data may be encoded and retrieved via standard file object methods,
   such as ``read``, ``readline`` and iteration:

   ::

      >>> csv_buffer.read(15)
      '1/2/09 6:17,Pro'

      >>> next(csv_buffer)
      'duct1,1200,Mastercard,carolina\r\n'

      >>> list(csv_buffer)
      ['1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

      >>> csv_buffer.read()
      ''

   Note, in the above example, we first read 15 bytes of the encoded
   CSV, then read the remainder of the line via iteration, (which
   invokes ``readline``), and then collected the remaining CSV into a
   list. Finally, we attempted to read the entirety still remaining –
   which was nothing.

**class ohio.CsvDictTextIO(rows, *writer_args, write_header=False,
chunk_size=10, **writer_kwargs)**

   ``CsvTextIO`` which accepts row data in the form of ``dict``.

   Data is passed to ``csv.DictWriter``.

   See also: ``ohio.CsvTextIO``.

**ohio.iter_csv(rows, *writer_args, write_header=False,
**writer_kwargs)**

   Generate lines of encoded CSV from ``rows`` of data.

   See: ``ohio.CsvWriterTextIO``.

**ohio.iter_dict_csv(rows, *writer_args, write_header=False,
**writer_kwargs)**

   Generate lines of encoded CSV from ``rows`` of data.

   See: ``ohio.CsvWriterTextIO``.

**class ohio.CsvWriterTextIO(*writer_args, **writer_kwargs)**

   csv.writer-compatible interface to iteratively encode CSV in
   memory.

   The writer instance may also be read, to retrieve written CSV, as
   it is written.

   Rather than write to the file system, an internal ``io.StringIO``
   buffer is used to store output temporarily, until it is read.
   (Unlike ``ohio.encode_csv``, this buffer is reused across
   read/write cycles.)

   Features class method ``iter_csv``: a generator to map an input
   iterable of data ``rows`` to lines of encoded CSV text.
   (``iter_csv`` differs from ``ohio.encode_csv`` in that it lazily
   generates lines of CSV, rather than eagerly encoding the entire CSV
   body.)

   **Note**: If you don’t need to control *how* rows are written, but
   do want an iterative and/or readable interface to encoded CSV,
   consider also the more straight-forward ``ohio.CsvTextIO``.

   For example, we may construct ``CsvWriterTextIO`` with the same
   (optional) arguments as we would ``csv.writer``, (minus the file
   descriptor):

   ::

      >>> csv_buffer = CsvWriterTextIO(dialect='excel')

   …and write to it, via either ``writerow`` or ``writerows``:

   ::

      >>> csv_buffer.writerows([
      ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
      ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
      ... ])

   Written data is then available to be read, via standard file object
   methods, such as ``read``, ``readline`` and iteration:

   ::

      >>> csv_buffer.read(15)
      '1/2/09 6:17,Pro'

      >>> list(csv_buffer)
      ['duct1,1200,Mastercard,carolina\r\n',
       '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

   Note, in the above example, we first read 15 bytes of the encoded
   CSV, and then collected the remaining CSV into a list, through
   iteration, (which returns its lines, via ``readline``). However,
   the first line was short by that first 15 bytes.

   That is, reading CSV out of the ``CsvWriterTextIO`` empties that
   content from its buffer:

   ::

      >>> csv_buffer.read()
      ''

   We can repopulate our ``CsvWriterTextIO`` buffer by writing to it
   again:

   ::

      >>> csv_buffer.writerows([
      ...     ('1/2/09 13:08', 'Product1', '1200', 'Mastercard', 'Federica e Andrea'),
      ...     ('1/3/09 14:44', 'Product1', '1200', 'Visa', 'Gouya'),
      ... ])

      >>> encoded_csv = csv_buffer.read()

      >>> encoded_csv[:80]
      '1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea\r\n1/3/09 14:44,Product1,1'

      >>> encoded_csv.splitlines(keepends=True)
      ['1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea\r\n',
       '1/3/09 14:44,Product1,1200,Visa,Gouya\r\n']

   Finally, class method ``iter_csv`` can do all this for us,
   generating lines of encoded CSV as we request them:

   ::

      >>> lines_csv = CsvWriterTextIO.iter_csv([
      ...     ('Transaction_date', 'Product', 'Price', 'Payment_Type', 'Name'),
      ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
      ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
      ...     ('1/2/09 13:08', 'Product1', '1200', 'Mastercard', 'Federica e Andrea'),
      ...     ('1/3/09 14:44', 'Product1', '1200', 'Visa', 'Gouya'),
      ... ])

      >>> next(lines_csv)
      'Transaction_date,Product,Price,Payment_Type,Name\r\n'

      >>> next(lines_csv)
      '1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n'

      >>> list(lines_csv)
      ['1/2/09 4:53,Product1,1200,Visa,Betina\r\n',
       '1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea\r\n',
       '1/3/09 14:44,Product1,1200,Visa,Gouya\r\n']

**class ohio.CsvDictWriterTextIO(*writer_args, **writer_kwargs)**

   ``CsvWriterTextIO`` which accepts row data in the form of ``dict``.

   Data is passed to ``csv.DictWriter``.

   See also: ``ohio.CsvWriterTextIO``.


iterio
------

Provide a readable file-like interface to any iterable.

**class ohio.IteratorTextIO(iterable)**

   Readable file-like interface for iterable text streams.

   ``IteratorTextIO`` wraps any iterable of text for consumption like
   a file, offering methods ``readline()``, ``read([size])``, *etc.*,
   (implemented via base class ``ohio.StreamTextIOBase``).

   For example, given a consumer which expects to ``read()``:

   ::

      >>> def read_chunks(fdesc, chunk_size=1024):
      ...     get_chunk = lambda: fdesc.read(chunk_size)
      ...     yield from iter(get_chunk, '')

   …And either streamed or in-memory text (*i.e.* which is not simply
   on a file system):

   ::

      >>> def all_caps(fdesc):
      ...     for line in fdesc:
      ...         yield line.upper()

   …We can connect these two interfaces via ``IteratorTextIO``:

   ::

      >>> with open('/usr/share/dict/words') as fdesc:
      ...     louder_words_lines = all_caps(fdesc)
      ...     with IteratorTextIO(louder_words_lines) as louder_words_desc:
      ...         louder_words_chunked = read_chunks(louder_words_desc)


pipeio
------

Efficiently connect ``read()`` and ``write()`` interfaces.

``PipeTextIO`` provides a *readable* and iterable interface to text
whose producer requires a *writable* interface.

In contrast to first writing such text to memory and then consuming
it, ``PipeTextIO`` only allows write operations as necessary to fill
its buffer, to fulfill read operations, asynchronously. As such,
``PipeTextIO`` consumes a stable minimum of memory, and may
significantly boost speed, with a minimum of boilerplate.

**ohio.pipe_text(writer_func, *args, buffer_size=None, **kwargs)**

   Iteratively stream output written by given function through
   readable file-like interface.

   Uses in-process writer thread, (which runs the given function), to
   mimic buffered text transfer, such as between the standard output
   and input of two piped processes.

   Calls to ``write`` are blocked until required by calls to ``read``.

   Note: If at all possible, use a generator! Your iterative text-
   writing function can most likely be designed as a generator, (or as
   some sort of iterator). Its output can then, far more simply and
   easily, be streamed to some input. If your input must be ``read``
   from a file-like object, see ``ohio.IteratorTextIO``. If your
   output must be CSV-encoded, see ``ohio.encode_csv``,
   ``ohio.CsvTextIO`` and ``ohio.CsvWriterTextIO``.

   ``PipeTextIO`` is suitable for situations where output *must* be
   written to a file-like object, which is made blocking to enforce
   iterativity.

   ``PipeTextIO`` is not “seekable,” but supports all other typical,
   read-write file-like features.

   For example, consider the following callable, (artificially)
   requiring a file-like object, to which to write:

   ::

      >>> def write_output(file_like):
      ...     file_like.write("Hi there.\r\n")
      ...     print('[writer]', 'Yay I wrote one line')
      ...     file_like.write("Cool, right?\r\n")
      ...     print('[writer]', 'Finally ... I wrote a second line!')
      ...     file_like.write("All right, later :-)\r\n")
      ...     print('[writer]', "Done.")

   Most typically, we might *read* this content as follows, using
   either the ``PipeTextIO`` constructor or its ``pipe_text`` helper:

   ::

      >>> with PipeTextIO(write_output) as pipe:
      ...     for line in pipe:
      ...         ...

   And, this syntax is recommended. However, for the sake of example,
   consider the following:

   ::

      >>> pipe = PipeTextIO(write_output, buffer_size=1)

      >>> pipe.read(5)
      [writer] Yay I wrote one line
      'Hi th'
      [writer] Finally ... I wrote a second line!

      >>> pipe.readline()
      'ere.\r\n'

      >>> pipe.readline()
      'Cool, right?\r\n'
      [writer] Done.

      >>> pipe.read()
      'All right, later :-)\r\n'

   In the above example, ``write_output`` requires a file-like
   interface to which to write its output; (and, we presume that there
   is no alternative to this implementation – such as a generator –
   that its output is large enough that we don’t want to hold it in
   memory **and** that we don’t need this output written to the file
   system). We are enabled to read it directly, in chunks:

   ..

      1. Initially, nothing is written.

      2. 1. Upon requesting to read – in this case, only the first 5
              bytes – the writer is initialized, and permitted to
              write its first chunk, (which happens to be one full
              line). This is retrieved from the write buffer, and
              sufficient to satisfy the read request.

          2. Having removed the first chunk from the write buffer,
              the writer is permitted to eagerly write its next chunk,
              (the second line), (but, no more than that).

      3. The second read request – for the remainder of the line – is
          fully satisfied by the first chunk retrieved from the write
          buffer. No more writing takes place.

      4. The third read request, for another line, retrieves the
          second chunk from the write buffer. The writer is permitted
          to write its final chunk to the write buffer.

      5. The final read request returns all remaining text,
          (retrieved from the write buffer).

   Concretely, this is commonly useful with the PostgreSQL COPY
   command, for efficient data transfer, (and without the added
   complexity of the file system). While your database interface may
   vary, ``PipeTextIO`` enables the following syntax, for example to
   copy data into the database:

   ::

      >>> def write_csv(file_like):
      ...     writer = csv.writer(file_like)
      ...     ...

      >>> with PipeTextIO(write_csv) as pipe, \
      ...      connection.cursor() as cursor:
      ...     cursor.copy_from(pipe, 'my_table', format='csv')

   …or, to copy data out of the database:

   ::

      >>> with connection.cursor() as cursor:
      ...     writer = lambda pipe: cursor.copy_to(pipe,
      ...                                          'my_table',
      ...                                          format='csv')
      ...
      ...     with PipeTextIO(writer) as pipe:
      ...         reader = csv.reader(pipe)
      ...         ...

   Alternatively, writer arguments may be passed to ``PipeTextIO``:

   ::

      >>> with connection.cursor() as cursor:
      ...     with PipeTextIO(cursor.copy_to,
      ...                     args=['my_table'],
      ...                     kwargs={'format': 'csv'}) as pipe:
      ...         reader = csv.reader(pipe)
      ...         ...

   (But, bear in mind, the signature of the callable passed to
   ``PipeTextIO`` must be such that its first, anonymous argument is
   the ``PipeTextIO`` instance.)

   Consider also the above example with the helper ``pipe_text``:

   ::

      >>> with connection.cursor() as cursor:
      ...     with pipe_text(cursor.copy_to,
      ...                    'my_table',
      ...                    format='csv') as pipe:
      ...         reader = csv.reader(pipe)
      ...         ...

   Finally, note that copying *to* the database is likely best
   performed via ``ohio.CsvTextIO``, (though copying *from* requires
   ``PipeTextIO``, as above):

   ::

      >>> with ohio.CsvTextIO(data_rows) as csv_buffer, \
      ...      connection.cursor() as cursor:
      ...     cursor.copy_from(csv_buffer, 'my_table', format='csv')


baseio
------

Low-level primitives.

**class ohio.StreamTextIOBase**

   Readable file-like abstract base class.

   Concrete classes must implement method ``__next_chunk__`` to return
   chunk(s) of the text to be read.

**exception ohio.IOClosed(*args)**

   Exception indicating an attempted operation on a file-like object
   which has been closed.

.. _extensions:


Extensions
----------

Modules integrating Ohio with the toolsets that need it.


Extensions for NumPy
~~~~~~~~~~~~~~~~~~~~

This module enables writing NumPy array data to database and
populating arrays from database via PostgreSQL ``COPY``. The operation
is ensured, by Ohio, to be memory-efficient.

**Note**: This integration is intended for NumPy, and attempts to
``import numpy``. NumPy must be available (installed) in your
environment.

**ohio.ext.numpy.pg_copy_to_table(arr, table_name, connectable,
columns=None, fmt=None)**

   Copy ``array`` to database table via PostgreSQL ``COPY``.

   ``ohio.PipeTextIO`` enables the direct, in-process “piping” of
   ``array`` CSV into the “standard input” of the PostgreSQL ``COPY``
   command, for quick, memory-efficient database persistence, (and
   without the needless involvement of the local file system).

   For example, given a SQLAlchemy ``connectable`` – either a database
   connection ``Engine`` or ``Connection`` – and a NumPy ``array``:

   ::

      >>> from sqlalchemy import create_engine
      >>> engine = create_engine('postgresql://')

      >>> arr = numpy.array([1.000102487, 5.982, 2.901, 103.929])

   We may persist this data to an existing table – *e.g.* “data”:

   ::

      >>> pg_copy_to_table(arr, 'data', engine, columns=['value'])

   ``pg_copy_to_table`` utilizes ``numpy.savetxt`` and supports its
   ``fmt`` parameter.

**ohio.ext.numpy.pg_copy_from_table(table_name, connectable, dtype,
columns=None)**

   Construct ``array`` from database table via PostgreSQL ``COPY``.

   ``ohio.PipeTextIO`` enables the in-process “piping” of the
   PostgreSQL ``COPY`` command into NumPy’s ``fromiter``, for quick,
   memory-efficient construction of ``array`` from database, (and
   without the needless involvement of the local file system).

   For example, given a SQLAlchemy ``connectable`` – either a database
   connection ``Engine`` or ``Connection``:

   ::

      >>> from sqlalchemy import create_engine
      >>> engine = create_engine('postgresql://')

   We may construct a NumPy ``array`` from the contents of a specified
   table:

   ::

      >>> arr = pg_copy_from_table(
      ...     'data',
      ...     engine,
      ...     float,
      ... )

**ohio.ext.numpy.pg_copy_from_query(query, connectable, dtype)**

   Construct ``array`` from database ``query`` via PostgreSQL
   ``COPY``.

   ``ohio.PipeTextIO`` enables the in-process “piping” of the
   PostgreSQL ``COPY`` command into NumPy’s ``fromiter``, for quick,
   memory-efficient construction of ``array`` from database, (and
   without the needless involvement of the local file system).

   For example, given a SQLAlchemy ``connectable`` – either a database
   connection ``Engine`` or ``Connection``:

   ::

      >>> from sqlalchemy import create_engine
      >>> engine = create_engine('postgresql://')

   We may construct a NumPy ``array`` from a given query:

   ::

      >>> arr = pg_copy_from_query(
      ...     'select value0, value1, value3 from data',
      ...     engine,
      ...     float,
      ... )


Extensions for Pandas
~~~~~~~~~~~~~~~~~~~~~

This module extends ``pandas.DataFrame`` with methods ``pg_copy_to``
and ``pg_copy_from``.

To enable, simply import this module anywhere in your project, (most
likely – just once, in its root module):

::

   >>> import ohio.ext.pandas

For example, if you have just one module – in there – or, in a Python
package:

::

   ohio/
       __init__.py
       baseio.py
       ...

then in its ``__init__.py``, to ensure that extensions are loaded
before your code, which uses them, is run.

**Note**: These extensions are intended for Pandas, and attempt to
``import pandas``. Pandas must be available (installed) in your
environment.

**class ohio.ext.pandas.DataFramePgCopyTo(data_frame)**

   ``pg_copy_to``: Copy ``DataFrame`` to database table via PostgreSQL
   ``COPY``.

   ``ohio.CsvTextIO`` enables the direct reading of ``DataFrame`` CSV
   into the “standard input” of the PostgreSQL ``COPY`` command, for
   quick, memory-efficient database persistence, (and without the
   needless involvement of the local file system).

   For example, given a SQLAlchemy ``connectable`` – either a database
   connection ``Engine`` or ``Connection`` – and a Pandas
   ``DataFrame``:

   ::

      >>> from sqlalchemy import create_engine
      >>> engine = create_engine('postgresql://')

      >>> df = pandas.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})

   We may simply invoke the ``DataFrame``’s Ohio extension method,
   ``pg_copy_to``:

   ::

      >>> df.pg_copy_to('users', engine)

   ``pg_copy_to`` supports all the same parameters as ``to_sql``,
   (excepting parameter ``method``).

**ohio.ext.pandas.to_sql_method_pg_copy_to(table, conn, keys,
data_iter)**

   Write pandas data to table via stream through PostgreSQL ``COPY``.

   This implements a pandas ``to_sql`` “method”, utilizing
   ``ohio.CsvTextIO`` for performance stability.

**ohio.ext.pandas.data_frame_pg_copy_from(sql, connectable,
schema=None, index_col=None, parse_dates=False, columns=None,
dtype=None, nrows=None, buffer_size=100)**

   ``pg_copy_from``: Construct ``DataFrame`` from database table or
   query via PostgreSQL ``COPY``.

   ``ohio.PipeTextIO`` enables the direct, in-process “piping” of the
   PostgreSQL ``COPY`` command into Pandas ``read_csv``, for quick,
   memory-efficient construction of ``DataFrame`` from database, (and
   without the needless involvement of the local file system).

   For example, given a SQLAlchemy ``connectable`` – either a database
   connection ``Engine`` or ``Connection``:

   ::

      >>> from sqlalchemy import create_engine
      >>> engine = create_engine('postgresql://')

   We may simply invoke the ``DataFrame``’s Ohio extension method,
   ``pg_copy_from``:

   ::

      >>> df = DataFrame.pg_copy_from('users', engine)

   ``pg_copy_from`` supports many of the same parameters as
   ``read_sql`` and ``read_csv``.

   In addition, ``pg_copy_from`` accepts the optimization parameter
   ``buffer_size``, which controls the maximum number of CSV-encoded
   results written by the database cursor to hold in memory prior to
   their being read into the ``DataFrame``. Depending on use-case,
   increasing this value may speed up the operation, at the cost of
   additional memory – and vice-versa. ``buffer_size`` defaults to
   ``100``.


Benchmarking
~~~~~~~~~~~~

Ohio extensions for pandas were benchmarked to test their speed and
memory-efficiency relative both to pandas built-in functionality and
to custom implementations which do not utilize Ohio.

Interfaces and syntactical niceties aside, Ohio generally features
memory stability. Its tools enable pipelines which may also improve
speed, (and which do so in standard use-cases).

In the below benchmark, Ohio extensions ``pg_copy_from`` &
``pg_copy_to`` reduced memory consumption by 84% & 61%, and completed
in 39% & 91% less time, relative to pandas built-ins ``read_sql`` &
``to_sql``, (respectively).

Compared to purpose-built extensions – which utilized PostgreSQL
``COPY``, but using ``io.StringIO`` in place of ``ohio.PipeTextIO``
and ``ohio.CsvTextIO`` – ``pg_copy_from`` & ``pg_copy_to`` also
reduced memory consumption by 60% & 32%, respectively.
``pg_copy_from`` & ``pg_copy_to`` also completed in 16% & 13% less
time than the ``io.StringIO`` versions.

The benchmarks plotted below were produced from averages and standard
deviations over 3 randomized trials per target. Input data consisted
of 896,677 rows across 83 columns: 1 of these of type timestamp, 51
integers and 31 floats. The benchmarking package, ``prof``, is
preserved in `Ohio's repository <https://github.com/dssg/ohio>`_.

.. image:: https://raw.githubusercontent.com/dssg/ohio/0.6.0/doc/img/profile-copy-from-database-to-datafram-1554345457.svg?sanitize=true

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

.. image:: https://raw.githubusercontent.com/dssg/ohio/0.6.0/doc/img/profile-copy-from-dataframe-to-databas-1555458507.svg?sanitize=true

ohio_pg_copy_to
   ``pg_copy_to()``

   ``DataFrame`` data are encoded through a ``CsvTextIO``, and read by
   a PostgreSQL database-connected cursor’s ``COPY`` command.

pandas_to_sql
   ``pandas.DataFrame.to_sql()``

   Pandas inserts ``DataFrame`` data into the database row by row.

pandas_to_sql_multi_100
   ``pandas.DataFrame.to_sql(method='multi', chunksize=100)``

   Pandas inserts ``DataFrame`` data into the database in chunks of
   rows.

copy_stringio_to_db
   ``DataFrame`` data are written and encoded to a ``StringIO``, and
   then read by a PostgreSQL database-connected cursor’s ``COPY``
   command.

.. _recipes:


Recipes
-------

Stand-alone modules implementing functionality which depends upon Ohio
primitives.


dbjoin
~~~~~~

Join the “COPY” results of arbitrary database queries in Python,
without unnecessary memory overhead.

This is largely useful to work around databases’ per-query column
limit.

**ohio.recipe.dbjoin.pg_join_queries(queries, engine, sep=', ',
end='\n', copy_options=('CSV', 'HEADER'))**

   Join the text-encoded result streams of an arbitrary number of
   PostgreSQL database queries to work around the database’s per-query
   column limit.

   Query results are read via PostgreSQL ``COPY``, streamed through
   ``PipeTextIO``, and joined line-by-line into a singular stream.

   For example, given a set of database queries whose results cannot
   be combined into a single PostgreSQL query, we might join these
   queries’ results and write these results to a file-like object:

   ::

      >>> queries = [
      ...     'SELECT a, b, c FROM a_table',
      ...     ...
      ... ]

      >>> with open('results.csv', 'w', newline='') as fdesc:
      ...     for line in pg_join_queries(queries, engine):
      ...         fdesc.write(line)

   Or, we might read these results into a single Pandas DataFrame:

   ::

      >>> csv_lines = pg_join_queries(queries, engine)
      >>> csv_buffer = ohio.IteratorTextIO(csv_lines)
      >>> df = pandas.read_csv(csv_buffer)

   By default, ``pg_join_queries`` requests CSV-encoded results, with
   an initial header line indicating the result columns. These
   options, which are sent directly to the PostgreSQL ``COPY``
   command, may be controlled via ``copy_options``. For example, to
   omit the CSV header:

   ::

      >>> pg_join_queries(queries, engine, copy_options=['CSV'])

   Or, to request PostgreSQL’s tab-delimited text format via the
   syntax of PostgreSQL v9.0+:

   ::

      >>> pg_join_queries(
      ...     queries,
      ...     engine,
      ...     sep='\t',
      ...     copy_options={'FORMAT': 'TEXT'},
      ... )

   In the above example, we’ve instructed PostgreSQL to use its
   ``text`` results encoder, (and we’ve omitted the instruction to
   include a header).

   **NOTE**: In the last example, we also explicitly specified the
   separator used in the results’ encoding. This is not passed to the
   database; rather, it is necessary for ``pg_join_queries`` to
   properly join queries’ results.
