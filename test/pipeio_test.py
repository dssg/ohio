import csv
import io
import time
import unittest.mock

import pytest
from timeout import timeout

import ohio

from . import ex_csv_stream, EXAMPLE_ROWS


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
