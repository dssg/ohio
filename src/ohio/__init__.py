"""
Oh! IO: The I/O tools that ``io`` doesn't want you to have.

Ohio provides the missing links between Python's built-in I/O
primitives, to help ensure the efficiency, clarity and elegance of your code.

"""
from .baseio import (IOClosed, StreamTextIOBase)
from .iterio import IteratorTextIO
from .csvio import (
    encode_csv,
    iter_csv,
    iter_dict_csv,
    CsvWriterTextIO,
    CsvDictWriterTextIO,
    CsvTextIO,
    CsvDictTextIO,
)
from .pipeio import PipeTextIO, pipe_text


__all__ = (
    'IOClosed',
    'StreamTextIOBase',
    'IteratorTextIO',
    'encode_csv',
    'iter_csv',
    'iter_dict_csv',
    'CsvWriterTextIO',
    'CsvDictWriterTextIO',
    'CsvTextIO',
    'CsvDictTextIO',
    'PipeTextIO',
    'pipe_text',
)


__version__ = '0.4.0'
