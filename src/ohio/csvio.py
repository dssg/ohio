"""
csvio
------

Flexibly encode data to CSV format.

"""
import csv
import io

from . import baseio


def csv_text(rows, *writer_args, writer=csv.writer, **writer_kwargs):
    r"""Encode the specified iterable of ``rows`` into CSV text.

    Data is encoded to an in-memory ``str``, (rather than to the file
    system), via an internally-managed ``io.StringIO``, (newly
    constructed for every invocation of ``csv_text``).

    For example::

        >>> data = [
        ...     ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
        ...     ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
        ... ]

        >>> encoded_csv = csv_text(data)

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

        >>> csv_text(data, writer=csv.DictWriter, fieldnames=header).splitlines(keepends=True)
        ['1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n',
         '1/2/09 4:53,Product1,1200,Visa,Betina\r\n']

    """
    out = io.StringIO()
    csv_writer = writer(out, *writer_args, **writer_kwargs)
    csv_writer.writerows(rows)
    return out.getvalue()


class CsvWriterTextIO(baseio.StreamTextIOBase):
    """csv.writer-compatible interface to encode CSV & write to memory.

    The writer instance may also be read, to retrieve written CSV, as
    it is written (iteratively).

    Rather than write to the file system, an internal ``io.StringIO``
    buffer is used to store output temporarily, until it is read.
    (Unlike ``ohio.csv_text``, this buffer is reused across read/write
    cycles.)

    """
    make_writer = csv.writer

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
