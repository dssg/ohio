import csv
import io

from . import baseio


def csv_text(rows, *args, writer=csv.writer, **kws):
    """Encode the specified iterable of `rows` into CSV text."""
    out = io.StringIO()
    csv_writer = writer(out, *args, **kws)
    csv_writer.writerows(rows)
    return out.getvalue()


class CsvWriterTextIO(baseio.StreamTextIOBase):
    """csv.writer-compatible interface to encode csv & write to memory.

    The writer instance may also be read, to retrieve written csv, as
    it is written (iteratively).

    """
    make_writer = csv.writer

    def __init__(self, *args, **kws):
        super().__init__()
        self.outfile = io.StringIO()
        self.writer = self.make_writer(self.outfile, *args, **kws)

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

    make_writer = csv.DictWriter

    def writeheader(self):
        self.writer.writeheader()
