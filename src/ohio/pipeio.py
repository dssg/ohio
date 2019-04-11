"""
pipeio
------

Efficiently connect ``read()`` and ``write()`` interfaces.

``PipeTextIO`` provides a *readable* and iterable interface to text
whose producer requires a *writable* interface.

In contrast to first writing such text to memory and then consuming it,
``PipeTextIO`` only allows write operations as necessary to fill its
buffer, to fulfill read operations, asynchronously. As such,
``PipeTextIO`` consumes a stable minimum of memory, and may
significantly boost speed, with a minimum of boilerplate.

"""
import queue
import threading

from . import baseio


class PipeTextIO(baseio.StreamTextIOBase):
    r"""Iteratively stream output written by given function through
    readable file-like interface.

    Uses in-process writer thread, (which runs the given function), to
    mimic buffered text transfer, such as between the standard output
    and input of two piped processes.

    Calls to ``write`` are blocked until required by calls to ``read``.

    Note: If at all possible, use a generator! Your iterative text-
    writing function can most likely be designed as a generator, (or as
    some sort of iterator). Its output can then, far more simply and
    easily, be streamed to some input. If your input must be ``read``
    from a file-like object, see ``ohio.IteratorTextIO``. If your output
    must be CSV-encoded, see ``ohio.csv_text`` and
    ``ohio.CsvWriterTextIO``.

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
    interface to which to write its output; and, we presume that there
    is no alternative to this implementation, (such as a generator),
    **and** that its output is large enough that we don't want to hold
    it in memory. And, in the case that we don't want this output
    written to the file system, we are enabled to read it directly, in
    chunks.

        1.  Initially, nothing is written.

        2.  a) Upon requesting to read -- in this case, only the first 5
               bytes -- the writer is initialized, and permitted to write
               its first chunk, (which happens to be one full line). This is
               retrieved from the write buffer, and sufficient to satisfy
               the read request.

            b) Having removed the first chunk from the write buffer, the
               writer is permitted to eagerly write its next chunk, (the
               second line), (but, no more than that).

        3.  The second read request -- for the remainder of the line --
            is fully satisfied by the first chunk retrieved from the
            write buffer. No more writing takes place.

        4.  The third read request, for another line, retrieves the
            second chunk from the write buffer. The writer is permitted
            to write its final chunk to the write buffer.

        5.  The final read request returns all remaining text,
            (retrieved from the write buffer).

    Concretely, this is commonly useful with the PostgreSQL COPY
    command, for efficient data transfer, (and without the added
    complexity of the file system). While your database interface may
    vary, ``PipeTextIO`` enables the following syntax, for example to
    copy data into the database::

        >>> def write_csv(file_like):
        ...     writer = csv.writer(file_like)
        ...     ...

        >>> with PipeTextIO(write_csv) as pipe, \
        ...      connection.cursor() as cursor:
        ...     cursor.copy_from(pipe, 'my_table', format='csv')

    ...or, to copy data out of the database::

        >>> with connection.cursor() as cursor:
        ...     writer = lambda pipe: cursor.copy_to(pipe,
        ...                                          'my_table',
        ...                                          format='csv')
        ...
        ...     with PipeTextIO(writer) as pipe:
        ...         reader = csv.reader(pipe)
        ...         ...

    Alternatively, writer arguments may be passed to ``PipeTextIO``::

        >>> with connection.cursor() as cursor:
        ...     with PipeTextIO(cursor.copy_to,
        ...                     args=['my_table'],
        ...                     kwargs={'format': 'csv'}) as pipe:
        ...         reader = csv.reader(pipe)
        ...         ...

    (But, bear in mind, the signature of the callable passed to
    ``PipeTextIO`` must be such that its first, anonymous argument is
    the ``PipeTextIO`` instance.)

    Consider also the above example with the helper ``pipe_text``::

        >>> with connection.cursor() as cursor:
        ...     with pipe_text(cursor.copy_to,
        ...                    'my_table',
        ...                    format='csv') as pipe:
        ...         reader = csv.reader(pipe)
        ...         ...

    """
    _log_debug = False
    _none = object()

    buffer_queue_size = 10
    queue_wait_timeout = 0.01

    thread_daemon = True

    @classmethod
    def _print_log(cls, where, message='', *message_args):
        if cls._log_debug:
            print('[debug]', '[%s]' % where, message % message_args)

    def __init__(self, writer_func, args=None, kwargs=None, buffer_size=None):
        super().__init__()

        self.__writer_func__ = writer_func
        self.__writer_args__ = args
        self.__writer_kwargs__ = kwargs

        self.buffer_queue_size = buffer_size or self.buffer_queue_size
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
        args = self.__writer_args__ or ()
        kwargs = self.__writer_kwargs__ or {}
        try:
            self.__writer_func__(self, *args, **kwargs)
        except baseio.IOClosed:
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
            raise baseio.IOClosed()

        self._buffer_queue.put(text)

        text_len = len(text)
        self._print_log('write', '%s', text_len)
        return text_len

    def writable(self):
        if self.closed:
            raise baseio.IOClosed()

        return True


def pipe_text(writer_func, *args, buffer_size=None, **kwargs):
    return PipeTextIO(
        writer_func,
        args=args,
        kwargs=kwargs,
        buffer_size=buffer_size,
    )


pipe_text.__doc__ = PipeTextIO.__doc__
