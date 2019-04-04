from . import baseio


class IteratorIO(baseio.StreamIOBase):
    """Readable file-like interface for iterable streams."""

    def __init__(self, iterable):
        super().__init__()
        self.__iterator__ = iter(iterable)

    def __next_chunk__(self):
        return next(self.__iterator__)


class IteratorTextIO(IteratorIO, baseio.StreamTextIOBase):
    pass

class IteratorBufferedIO(IteratorIO, baseio.StreamBufferedIOBase):
    pass
