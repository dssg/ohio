import csv
import io
import itertools

import pytest

import ohio

from . import ex_csv_stream, EXAMPLE_ROWS


class TestCsvText:

    def test_writer(self):
        # iterator NOT required; rather, test that iterator ALLOWED:
        csv_input = iter(EXAMPLE_ROWS)
        csv_content = ''.join(ex_csv_stream())
        assert ohio.csv_text(csv_input) == csv_content

    def test_dictwriter(self):
        csv_input = iter(EXAMPLE_ROWS)
        field_names = next(csv_input)
        dict_input = (dict(zip(field_names, row)) for row in csv_input)

        csv_stream = ex_csv_stream()
        # note: currently no handling for `writeheader` in this version
        next(csv_stream)  # skip header
        csv_content = ''.join(csv_stream)

        assert ohio.csv_text(
            dict_input,
            fieldnames=field_names,
            writer=csv.DictWriter,
        ) == csv_content


class TestCsvWriterTextIO:

    @pytest.fixture
    def buffer(self):
        return ohio.CsvWriterTextIO()

    def test_writerow(self, buffer):
        for row in EXAMPLE_ROWS[:2]:
            buffer.writerow(row)

        csv_stream = ex_csv_stream()
        csv_content = ''.join(itertools.islice(csv_stream, 2))

        assert buffer.read() == csv_content

        assert buffer.read() == ''

        buffer.writerow(EXAMPLE_ROWS[2])
        assert buffer.read() == next(csv_stream)

    def test_not_seekable(self, buffer):
        assert not buffer.seekable()

    def test_not_writable(self, buffer):
        assert not buffer.writable()

    @pytest.mark.parametrize('method_name,method_args', (
        ('seek', ()),
        ('tell', ()),
        ('truncate', ()),
        ('write', ()),
        ('writelines', (['hi\n'],)),
    ))
    def test_write_methods(self, buffer, method_name, method_args):
        method = getattr(buffer, method_name)

        with pytest.raises(io.UnsupportedOperation):
            method(*method_args)


class TestCsvDictWriterTextIO:

    @pytest.fixture
    def buffer(self):
        return ohio.CsvDictWriterTextIO(fieldnames=EXAMPLE_ROWS[0])

    def test_writerow(self, buffer):
        buffer.writeheader()
        buffer.writerow(dict(zip(*EXAMPLE_ROWS[:2])))

        csv_stream = ex_csv_stream()
        csv_content = ''.join(itertools.islice(csv_stream, 2))

        assert buffer.read() == csv_content

        assert buffer.read() == ''

        buffer.writerow(dict(zip(EXAMPLE_ROWS[0], EXAMPLE_ROWS[2])))
        assert buffer.read() == next(csv_stream)
