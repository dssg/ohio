import csv
import io
import itertools
import time
import unittest.mock

import pytest
from timeout import timeout

import ohio


EXAMPLE_ROWS = (
    ('Transaction_date', 'Product', 'Price', 'Payment_Type', 'Name'),
    ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
    ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
    ('1/2/09 13:08', 'Product1', '1200', 'Mastercard', 'Federica e Andrea'),
    ('1/3/09 14:44', 'Product1', '1200', 'Visa', 'Gouya'),
    ('1/4/09 12:56', 'Product2', '3600', 'Visa', 'Gerd W '),
    ('1/4/09 13:19', 'Product1', '1200', 'Visa', 'LAURENCE'),
    ('1/4/09 20:11', 'Product1', '1200', 'Mastercard', 'Fleur'),
    ('1/2/09 20:09', 'Product1', '1200', 'Mastercard', 'adam'),
    ('1/4/09 13:17', 'Product1', '1200', 'Mastercard', 'Renee Elisabeth'),
)


def ex_csv_stream():
    yield 'Transaction_date,Product,Price,Payment_Type,Name\r\n'
    yield '1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n'
    yield '1/2/09 4:53,Product1,1200,Visa,Betina\r\n'
    yield '1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea\r\n'
    yield '1/3/09 14:44,Product1,1200,Visa,Gouya\r\n'
    yield '1/4/09 12:56,Product2,3600,Visa,Gerd W \r\n'
    yield '1/4/09 13:19,Product1,1200,Visa,LAURENCE\r\n'
    yield '1/4/09 20:11,Product1,1200,Mastercard,Fleur\r\n'
    yield '1/2/09 20:09,Product1,1200,Mastercard,adam\r\n'
    yield '1/4/09 13:17,Product1,1200,Mastercard,Renee Elisabeth\r\n'


class TestIteratorTextIO:

    @pytest.fixture
    def csv_stream(self):
        # only necessary to *spy* on iteration (calls to __next__)
        iter_mock = unittest.mock.MagicMock(**{
            'return_value.__next__.side_effect': ex_csv_stream(),
        })
        return unittest.mock.Mock(__iter__=iter_mock)

    @pytest.fixture
    def buffer(self, csv_stream):
        return ohio.IteratorTextIO(csv_stream)

    def test_context_manager(self, buffer):
        assert not buffer.closed

        with buffer as buffer1:
            assert buffer is buffer1
            assert not buffer.closed

        assert buffer.closed

    def test_readable(self, buffer):
        assert buffer.readable()

    def test_readable_closed(self, buffer):
        buffer.close()

        with pytest.raises(ohio.IOClosed):
            buffer.readable()

    def test_read(self, buffer):
        all_content = ''.join(ex_csv_stream())
        assert buffer.read() == all_content
        assert buffer.__iterator__.__next__.call_count == 11

    def test_read_closed(self, buffer):
        buffer.close()

        with pytest.raises(ohio.IOClosed):
            buffer.read()

    def test_read_parts(self, buffer):
        for (iteration, size, chunk) in (
            (1, 5, 'Trans'),
            (1, 15, 'action_date,Pro'),
            (2, 43, 'duct,Price,Payment_Type,Name\r\n1/2/09 6:17,P'),
        ):
            assert buffer.read(size) == chunk
            assert buffer.__iterator__.__next__.call_count == iteration

        assert buffer.read(None)
        assert buffer.__iterator__.__next__.call_count == 11

    def test_readline(self, buffer):
        for (count, line) in enumerate(ex_csv_stream(), 1):
            assert buffer.readline() == line
            assert buffer.__iterator__.__next__.call_count == count

    def test_readline_closed(self, buffer):
        buffer.close()

        with pytest.raises(ohio.IOClosed):
            buffer.readline()

    def test_readlines(self, buffer):
        assert buffer.readlines() == list(ex_csv_stream())
        assert buffer.__iterator__.__next__.call_count == 11

    def test_iter(self, buffer):
        for (count, (buffer_line, example_line)) in enumerate(zip(buffer, ex_csv_stream()), 1):
            assert buffer_line == example_line
            assert buffer.__iterator__.__next__.call_count == count

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


class TestPipeTextIO:

    race_timeout = 0.2

    class RecordedStreamWriter:

        def __init__(self):
            self.write_count = 0

        def __call__(self, outfile):
            csv_writer = csv.writer(outfile)
            for row in EXAMPLE_ROWS:
                csv_writer.writerow(row)
                self.write_count += 1

    @pytest.fixture
    def stream_writer(self):
        return self.RecordedStreamWriter()

    @pytest.fixture
    def pipe(self, stream_writer):
        return ohio.PipeTextIO(stream_writer)

    def test_context_manager(self, pipe):
        assert not pipe.closed

        with pipe as pipe1:
            assert pipe is pipe1
            assert not pipe.closed

        assert pipe.closed

    def test_readable(self, pipe):
        assert pipe.readable()

    def test_readable_closed(self, pipe):
        pipe.close()

        with pytest.raises(ohio.IOClosed):
            pipe.readable()

    # repeat test_read 5 times to ensure race condition triggered
    @timeout(race_timeout)
    @pytest.mark.parametrize('trial', range(5))
    def test_read(self, pipe, stream_writer, trial):
        assert stream_writer.write_count == 0

        all_content = ''.join(ex_csv_stream())
        assert pipe.read() == all_content

        assert stream_writer.write_count == 10

    def test_read_closed(self, pipe):
        pipe.close()

        with pytest.raises(ohio.IOClosed):
            pipe.read()

    @timeout(race_timeout)
    def test_read_parts(self, pipe, stream_writer):
        assert stream_writer.write_count == 0
        assert not pipe._writer.is_alive()

        for (write_count, size, chunk) in (
            (2, 5, 'Trans'),
            (2, 15, 'action_date,Pro'),
            (3, 43, 'duct,Price,Payment_Type,Name\r\n1/2/09 6:17,P'),
        ):
            assert pipe.read(size) == chunk

            # handle race condition in count synchronization
            assert stream_writer.write_count <= write_count

        assert pipe._writer.is_alive()

        assert pipe.read(None)

        assert stream_writer.write_count == 10
        assert not pipe._writer.is_alive()

    @timeout(race_timeout)
    def test_readline(self, pipe, stream_writer):
        assert stream_writer.write_count == 0

        for (count, line) in enumerate(ex_csv_stream(), 2):
            assert pipe.readline() == line
            assert stream_writer.write_count <= count

        assert stream_writer.write_count == 10

    def test_readline_closed(self, pipe):
        pipe.close()

        with pytest.raises(ohio.IOClosed):
            pipe.readline()

    @timeout(race_timeout)
    def test_readlines(self, pipe, stream_writer):
        assert stream_writer.write_count == 0
        assert pipe.readlines() == list(ex_csv_stream())
        assert stream_writer.write_count == 10

    @timeout(race_timeout)
    def test_iter(self, pipe, stream_writer):
        assert stream_writer.write_count == 0

        for (count, (pipe_line, example_line)) in enumerate(zip(pipe, ex_csv_stream()), 2):
            assert pipe_line == example_line
            assert stream_writer.write_count <= count

        assert stream_writer.write_count == 10

    def test_not_seekable(self, pipe):
        assert not pipe.seekable()

    def test_writable(self, pipe):
        assert pipe.writable()

    @pytest.mark.parametrize('method_name', ('seek', 'tell', 'truncate'))
    def test_unsupported_methods(self, pipe, method_name):
        method = getattr(pipe, method_name)

        with pytest.raises(io.UnsupportedOperation):
            method()

    def test_read_some_close(self, pipe, stream_writer):
        assert stream_writer.write_count == 0
        assert not pipe._writer.is_alive()

        assert pipe.read(5) == 'Trans'
        assert stream_writer.write_count <= 2
        assert pipe._writer.is_alive()

        pipe.close()

        # wait (with timeout)
        for _count in range(20):
            if not pipe._writer.is_alive():
                break

            time.sleep(0.01)

        assert not pipe._writer.is_alive()
        assert stream_writer.write_count <= 2

        with pytest.raises(ohio.IOClosed):
            pipe.read()

        assert stream_writer.write_count <= 2
        assert not pipe._writer.is_alive()

    @staticmethod
    def broken_writer(pipe):
        pipe.write("hi there\r\n")
        pipe.write("what's up?\r\n")
        pipe.write("(not much)\r\n")
        raise RuntimeError('ah!')

    @timeout(race_timeout)
    @pytest.mark.parametrize('writer', (
        # one writer which fails immediately:
        unittest.mock.Mock(side_effect=RuntimeError),

        # another writer which fails eventually:
        broken_writer.__func__,
    ))
    def test_writer_exception(self, writer):
        with ohio.PipeTextIO(writer) as pipe:
            with pytest.raises(RuntimeError):
                pipe.read()

            # wait (with timeout)
            for _count in range(20):
                if not pipe._writer.is_alive():
                    break

                time.sleep(0.01)

            assert not pipe._writer.is_alive()
            assert not pipe.closed

        assert pipe.closed
