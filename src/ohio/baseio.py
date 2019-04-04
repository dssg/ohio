import io


class IOClosed(ValueError):

    _default_message_ = "I/O operation on closed file"

    def __init__(self, *args):
        if not args:
            args = (self._default_message_,)

        super().__init__(*args)


class StreamIOBase(object):
    """Readable file-like abstract base class.

    Concrete classes may implemented method `__next_chunk__` to return
    chunks (or all) of the text to be read.

    """
    def __next_chunk__(self):
        raise NotImplementedError("StreamIOBase subclasses must implement __next_chunk__")

    def _get_empty_value(self):
        raise NotImplementedError("StreamIOBase subclasses must implement _get_empty_value")

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

        result = self._get_empty_value()

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

        result = self._get_empty_value()
        newline = self._get_newline()

        while True:
            index = self._remainder.find(newline)
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


class StreamTextIOBase(StreamIOBase, io.TextIOBase):
    """Readable file-like abstract base class for text.

    Concrete classes may implemented method `__next_chunk__` to return
    chunks (or all) of the text to be read.

    """

    def __init__(self):
        self._remainder = ''

    @staticmethod
    def _get_empty_value():
        return ''

    @staticmethod
    def _get_newline():
        return '\n'


class StreamBufferedIOBase(StreamIOBase, io.BufferedIOBase):
    """Readable file-like abstract base class for bytes.

    Concrete classes may implemented method `__next_chunk__` to return
    chunks (or all) of the bytes to be read.
    """
    def __init__(self):
        self._remainder = b''

    @staticmethod
    def _get_empty_value():
        return b''

    @staticmethod
    def _get_newline():
        return b'\n'
