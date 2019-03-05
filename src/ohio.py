"""I/O extras"""
import csv
import io
import queue
import threading


__version__ = '0.1.2'


class IOClosed(ValueError):

    _default_message_ = "I/O operation on closed file"

    def __init__(self, *args):
        if not args:
            args = (self._default_message_,)

        super().__init__(*args)


class StreamTextIOBase(io.TextIOBase):
    """Readable file-like abstract base class.

    Concrete classes may implemented method `__next_chunk__` to return
    chunks (or all) of the text to be read.

    """
    def __init__(self):
        self._remainder = ''

    def __next_chunk__(self):
        raise NotImplementedError("StreamTextIOBase subclasses must implement __next_chunk__")

    def readable(self):
        if self.closed:
            raise IOClosed()

        return True

    def _read1(self, size=None):
        while not self._remainder:
            try:
                self._remainder = self.__next_chunk__()
            except StopIteration:
                break

        result = self._remainder[:size]
        self._remainder = self._remainder[len(result):]

        return result

    def read(self, size=None):
        if self.closed:
            raise IOClosed()

        if size is not None and size < 0:
            size = None

        result = ''

        while size is None or size > 0:
            content = self._read1(size)
            if not content:
                break

            if size is not None:
                size -= len(content)

            result += content

        return result

    def readline(self):
        if self.closed:
            raise IOClosed()

        result = ''

        while True:
            index = self._remainder.find('\n')
            if index == -1:
                result += self._remainder
                try:
                    self._remainder = self.__next_chunk__()
                except StopIteration:
                    self._remainder = ''
                    break
            else:
                result += self._remainder[:(index + 1)]
                self._remainder = self._remainder[(index + 1):]
                break

        return result


class IteratorTextIO(StreamTextIOBase):
    """Readable file-like interface for iterable text streams."""

    def __init__(self, iterable):
        super().__init__()
        self.__iterator__ = iter(iterable)

    def __next_chunk__(self):
        return next(self.__iterator__)


def csv_text(rows, *args, writer=csv.writer, **kws):
    """Encode the specified iterable of `rows` into CSV text."""
    out = io.StringIO()
    csv_writer = writer(out, *args, **kws)
    csv_writer.writerows(rows)
    return out.getvalue()


class CsvWriterTextIO(StreamTextIOBase):
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


class PipeTextIO(StreamTextIOBase):
    """Iteratively stream output written by given function through
    readable file-like interface.

    Uses in-process writer thread, (which runs the given function), to
    mimic buffered text transfer, such as between the standard output
    and input of two piped processes.

    Calls to ``write`` are blocked until required by calls to ``read``.

    Note: If at all possible, use a generator! Your iterative text-
    writing function can most likely be designed as a generator, (or as
    some sort of iterator). Its output can then, far more simply and
    easily, be streamed to some input. If your input must be ``read``
    from a file-like object, see ``IteratorTextIO``. If your output must
    be CSV-encoded, see ``csv_text`` and ``CsvWriterTextIO``.

    ``PipeTextIO`` is suitable for situations where output *must* be
    written to a file-like object, which is made blocking to enforce
    iterativity.

    ``PipeTextIO`` is not "seekable," but supports all other typical,
    read-write file-like features.

    For example, consider the following callable, requiring a file-like
    object, to which to write::

        >>> def write_output(file_like):
        ...     file_like.write("Hi there.\r\n")
        ...     print('[writer]', 'Yay I wrote one line')
        ...     file_like.write("Cool, right?\r\n")
        ...     print('[writer]', 'Finally ... I wrote a second line!')
        ...     file_like.write("All right, later :-)\r\n")
        ...     print('[writer]', "Done.")

    Most typically, we might *read* this content as follows::

        >>> with PipeTextIO(write_output) as pipe:
        ...     for line in pipe:
        ...         ...

    And, this is recommended. However, for the sake of example,
    consider the following::

        >>> pipe = PipeTextIO(write_output)

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
    interface to which to write its output; and, we presume that there
    is no alternative to this implementation, (such as a generator),
    **and** that its output is large enough that we don't want to hold
    it in memory. And, in the case that we don't want this output
    written to the file system, we are enabled to read it directly, in
    chunks.

        0.  Initially, nothing is written.
        1a. Upon requesting to read -- in this case, only the first 5
            bytes -- the writer is initialized, and permitted to write
            its first chunk, (which happens to be one full line). This is
            retrieved from the write buffer, and sufficient to satisfy
            the read request.
        1b. Having removed the first chunk from the write buffer, the
            writer is permitted to eagerly write its next chunk, (the
            second line), (but, no more than that).
        2.  The second read request -- for the remainder of the line --
            is fully satisfied by the first chunk retrieved from the
            write buffer. No more writing takes place.
        3.  The third read request, for another line, retrieves the
            second chunk from the write buffer. The writer is permitted
            to write its final chunk to the write buffer.
        4.  The final read request returns all remaining text,
            (retrieved from the write buffer).

    """
    _log_debug = False
    _none = object()

    buffer_queue_size = 1
    queue_wait_timeout = 0.01

    thread_daemon = True

    @classmethod
    def _print_log(cls, where, message='', *message_args):
        if cls._log_debug:
            print('[debug]', '[%s]' % where, message % message_args)

    def __init__(self, writer_func):
        super().__init__()

        self.__writer_func__ = writer_func

        self._buffer_queue = queue.Queue(self.buffer_queue_size)

        self._writer = threading.Thread(
            daemon=self.thread_daemon,
            target=self._writer_write,
        )
        self._writer_started = False
        self._writer_exc = None

    @property
    def _should_wait(self):
        return not self._writer_started or self._writer.is_alive()

    def _ensure_started(self):
        if not self._writer_started:
            self._writer.start()
            self._writer_started = True
            self._print_log('writer', 'started')

    def __next_chunk__(self):
        self._print_log('read')

        self._ensure_started()

        #
        # There is a race condition between:
        #
        #   * checking ``_should_wait`` -- (whether worker alive and could enqueue more)
        #   * worker finishing up and "dying"
        #
        while True:
            # handle race condition on checking Thread.is_alive
            try:
                text = self._buffer_queue.get(self._should_wait,
                                              self.queue_wait_timeout)
            except queue.Empty:
                if not self._should_wait:
                    text = self._none
                    break
            else:
                break

        if self._writer_exc:
            raise self._writer_exc

        if text is self._none:
            raise StopIteration

        self._buffer_queue.task_done()

        self._print_log('read', '%r', text)
        return text

    def _writer_write(self):
        try:
            self.__writer_func__(self)
        except IOClosed:
            self._print_log('writer', 'killed')
        except Exception as exc:
            self._print_log('writer', 'error: %r', exc)
            self._writer_exc = exc
        else:
            self._print_log('writer', 'done')

    def close(self):
        super().close()

        # empty queue / trigger writer (and thereby kill thread)
        if self._buffer_queue.full():
            self._buffer_queue.get_nowait()

    def write(self, text):
        self._print_log('write', '%r', text)

        if self.closed:
            raise IOClosed()

        self._buffer_queue.put(text)

        text_len = len(text)
        self._print_log('write', '%s', text_len)
        return text_len

    def writable(self):
        if self.closed:
            raise IOClosed()

        return True
