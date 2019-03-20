import io
import unittest.mock

import pytest

import ohio

from . import ex_csv_stream


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
