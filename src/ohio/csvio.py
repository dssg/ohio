"""
csvio
------

Flexibly encode data to CSV format.

"""
import csv
import io
import itertools

from . import baseio


def encode_csv(rows, *writer_args, writer=csv.writer, write_header=False, **writer_kwargs):
    r"""Encode the specified iterable of ``rows`` into CSV text.

    Data is encoded to an in-memory ``str``, (rather than to the file
    system), via an internally-managed ``io.StringIO``, (newly
    constructed for every invocation of ``encode_csv``).

    For example::

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

    By default, ``rows`` are encoded by built-in ``csv.writer``. You may
    specify an alternate ``writer``, and provide construction arguments::

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
    instruct ``encode_csv`` to invoke this, prior to writing ``rows``::

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

    """
    out = io.StringIO()
    csv_writer = writer(out, *writer_args, **writer_kwargs)
    if write_header:
        csv_writer.writeheader()
    csv_writer.writerows(rows)
    return out.getvalue()


class CsvTextIO(baseio.StreamTextIOBase):
    r"""Readable file-like interface encoding specified data as CSV.

    Rows of input data are only consumed and encoded as needed, as
    ``CsvTextIO`` is read.

    Rather than write to the file system, an internal ``io.StringIO``
    buffer is used to store output temporarily, until it is read.
    (Also unlike ``ohio.encode_csv``, this buffer is reused across
    read/write cycles.)

    For example, we might encode the following data as CSV::

        >>> data = [
        ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
        ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
        ... ]

        >>> csv_buffer = CsvTextIO(data)

    Data may be encoded and retrieved via standard file object methods,
    such as ``read``, ``readline`` and iteration::

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

    """
    make_writer = csv.writer

    def __init__(self, rows, *writer_args, write_header=False, chunk_size=10, **writer_kwargs):
        super().__init__()
        self.rows = iter(rows)
        self.must_writeheader = write_header
        self.chunk_size = chunk_size
        self.outfile = io.StringIO()
        self.writer = self.make_writer(self.outfile, *writer_args, **writer_kwargs)

    def __next_chunk__(self):
        if self.must_writeheader:
            self.writer.writeheader()
            self.must_writeheader = False
        else:
            # split out writing of chunk's first row to ensure we raise
            # StopIteration if chunk will be empty;
            # (but having done so then allow chunk to simply be "short")
            self.writer.writerow(next(self.rows))

        if self.chunk_size > 1:
            row_remainder = itertools.islice(self.rows, (self.chunk_size - 1))
            self.writer.writerows(row_remainder)

        text = self.outfile.getvalue()

        self.outfile.seek(0)
        self.outfile.truncate()

        return text


class CsvDictTextIO(CsvTextIO):
    """``CsvTextIO`` which accepts row data in the form of ``dict``.

    Data is passed to ``csv.DictWriter``.

    See also: ``ohio.CsvTextIO``.

    """
    make_writer = csv.DictWriter


class CsvWriterTextIO(baseio.StreamTextIOBase):
    r"""csv.writer-compatible interface to iteratively encode CSV in
    memory.

    The writer instance may also be read, to retrieve written CSV, as
    it is written.

    Rather than write to the file system, an internal ``io.StringIO``
    buffer is used to store output temporarily, until it is read.
    (Unlike ``ohio.encode_csv``, this buffer is reused across read/write
    cycles.)

    Features class method ``iter_csv``: a generator to map an input
    iterable of data ``rows`` to lines of encoded CSV text.
    (``iter_csv`` differs from ``ohio.encode_csv`` in that it lazily
    generates lines of CSV, rather than eagerly encoding the entire CSV
    body.)

    **Note**: If you don't need to control *how* rows are written, but
    do want an iterative and/or readable interface to encoded CSV,
    consider also the more straight-forward ``ohio.CsvTextIO``.

    For example, we may construct ``CsvWriterTextIO`` with the same
    (optional) arguments as we would ``csv.writer``, (minus the file
    descriptor)::

        >>> csv_buffer = CsvWriterTextIO(dialect='excel')

    ...and write to it, via either ``writerow`` or ``writerows``::

        >>> csv_buffer.writerows([
        ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
        ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
        ... ])

    Written data is then available to be read, via standard file object
    methods, such as ``read``, ``readline`` and iteration::

        >>> csv_buffer.read(15)
        '1/2/09 6:17,Pro'

        >>> list(csv_buffer)
        ['duct1,1200,Mastercard,carolina\r\n',
         '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

    Note, in the above example, we first read 15 bytes of the encoded
    CSV, and then collected the remaining CSV into a list, through
    iteration, (which returns its lines, via ``readline``). However, the
    first line was short by that first 15 bytes.

    That is, reading CSV out of the ``CsvWriterTextIO`` empties that
    content from its buffer::

        >>> csv_buffer.read()
        ''

    We can repopulate our ``CsvWriterTextIO`` buffer by writing to it
    again::

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
    generating lines of encoded CSV as we request them::

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

    """
    make_writer = csv.writer

    @classmethod
    def iter_csv(cls, rows, *writer_args, write_header=False, **writer_kwargs):
        """Generate lines of encoded CSV from ``rows`` of data.

        See: ``ohio.CsvWriterTextIO``.

        """
        csv_buffer = cls(*writer_args, **writer_kwargs)

        if write_header:
            csv_buffer.writeheader()
            yield csv_buffer.read()

        for row in rows:
            csv_buffer.writerow(row)
            yield csv_buffer.read()

    def __init__(self, *writer_args, **writer_kwargs):
        super().__init__()
        self.outfile = io.StringIO()
        self.writer = self.make_writer(self.outfile, *writer_args, **writer_kwargs)

    # csv.writer interface #

    @property
    def dialect(self):
        return self.writer.dialect

    def writerow(self, row):
        self.writer.writerow(row)

    def writerows(self, rows):
        self.writer.writerows(rows)

    # StreamTextIOBase readable interface #

    def __next_chunk__(self):
        text = self.outfile.getvalue()

        if not text:
            # NOTE: Does StreamTextIOBase make the most sense for this?
            raise StopIteration

        self.outfile.seek(0)
        self.outfile.truncate()

        return text


class CsvDictWriterTextIO(CsvWriterTextIO):
    """``CsvWriterTextIO`` which accepts row data in the form of
    ``dict``.

    Data is passed to ``csv.DictWriter``.

    See also: ``ohio.CsvWriterTextIO``.

    """
    make_writer = csv.DictWriter

    def writeheader(self):
        self.writer.writeheader()


iter_csv = CsvWriterTextIO.iter_csv

iter_dict_csv = CsvDictWriterTextIO.iter_csv
