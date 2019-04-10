"""
iterio
------

Provide a readable file-like interface to any iterable.

"""
from . import baseio


class IteratorTextIO(baseio.StreamTextIOBase):
    """Readable file-like interface for iterable text streams.

    ``IteratorTextIO`` wraps any iterable of text for consumption like a
    file, offering methods ``readline()``, ``read([size])``, *etc.*,
    (implemented via base class ``ohio.StreamTextIOBase``).

    For example, given a consumer which expects to ``read()``::

        >>> def read_chunks(fdesc, chunk_size=1024):
        ...     get_chunk = lambda: fdesc.read(chunk_size)
        ...     yield from iter(get_chunk, '')

    ...And either streamed or in-memory text (*i.e.* which is not simply
    on a file system)::

        >>> def all_caps(fdesc):
        ...     for line in fdesc:
        ...         yield line.upper()

    ...We can connect these two interfaces via ``IteratorTextIO``::

        >>> with open('/usr/share/dict/words') as fdesc:
        ...     louder_words_lines = all_caps(fdesc)
        ...     with IteratorTextIO(louder_words_lines) as louder_words_desc:
        ...         louder_words_chunked = read_chunks(louder_words_desc)

    """
    def __init__(self, iterable):
        super().__init__()
        self.__iterator__ = iter(iterable)

    def __next_chunk__(self):
        return next(self.__iterator__)
