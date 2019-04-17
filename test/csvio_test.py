import csv
import io
import itertools

import pytest

import ohio

from . import ex_csv_stream, EXAMPLE_ROWS


class TestEncodeCsv:

    def test_writer(self):
        # iterator NOT required; rather, test that iterator ALLOWED:
        csv_input = iter(EXAMPLE_ROWS)
        csv_content = ''.join(ex_csv_stream())
        assert ohio.encode_csv(csv_input) == csv_content

    @pytest.mark.parametrize('write_header', (False, True))
    def test_dictwriter(self, write_header):
        csv_input = iter(EXAMPLE_ROWS)
        field_names = next(csv_input)
        dict_input = (dict(zip(field_names, row)) for row in csv_input)

        csv_stream = ex_csv_stream()
        if not write_header:
            next(csv_stream)  # skip header
        csv_content = ''.join(csv_stream)

        assert ohio.encode_csv(
            dict_input,
            fieldnames=field_names,
            writer=csv.DictWriter,
            write_header=write_header,
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

    def test_iter_csv(self):
        csv_lines = ohio.CsvWriterTextIO.iter_csv(EXAMPLE_ROWS)
        for (actual_line, expected_line) in zip(csv_lines, ex_csv_stream()):
            assert actual_line == expected_line


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

    def test_iter_csv(self):
        fieldnames = EXAMPLE_ROWS[0]
        rows = (dict(zip(fieldnames, row)) for row in EXAMPLE_ROWS[1:])
        csv_lines = ohio.CsvDictWriterTextIO.iter_csv(
            rows,
            fieldnames=fieldnames,
            write_header=True,
        )
        for (actual_line, expected_line) in zip(csv_lines, ex_csv_stream()):
            assert actual_line == expected_line


class TestCsvTextIO:

    @pytest.fixture
    def buffer(self):
        return ohio.CsvTextIO(EXAMPLE_ROWS)

    def test_read(self, buffer):
        assert buffer.read() == ''.join(ex_csv_stream())

    def test_read_parts(self, buffer):
        expected = ''.join(ex_csv_stream())

        assert buffer.read(15) == expected[:15]

        assert buffer.read() == expected[15:]

    def test_read_lines(self, buffer):
        for (actual_line, expected_line) in zip(buffer, ex_csv_stream()):
            assert actual_line == expected_line

    def test_read_lines_cm(self, buffer):
        with buffer as buffer1:
            assert buffer1 is buffer

            for (actual_line, expected_line) in zip(buffer, ex_csv_stream()):
                assert actual_line == expected_line

            assert buffer.read() == ''

        with pytest.raises(ohio.IOClosed):
            buffer.read()


class TestCsvDictTextIO:

    @pytest.fixture
    def buffer(self):
        fieldnames = EXAMPLE_ROWS[0]
        rows = (dict(zip(fieldnames, row)) for row in EXAMPLE_ROWS[1:])
        return ohio.CsvDictTextIO(
            rows,
            fieldnames=fieldnames,
            write_header=True,
        )

    def test_read(self, buffer):
        assert buffer.read() == ''.join(ex_csv_stream())
