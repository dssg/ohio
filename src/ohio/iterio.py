from . import baseio


class IteratorTextIO(baseio.StreamTextIOBase):
    """Readable file-like interface for iterable text streams."""

    def __init__(self, iterable):
        super().__init__()
        self.__iterator__ = iter(iterable)

    def __next_chunk__(self):
        return next(self.__iterator__)
