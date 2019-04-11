"""
Oh! IO: The I/O tools that ``io`` doesn't want you to have.

Ohio provides the missing links between Python's built-in I/O
primitives, to help ensure the efficiency, clarity and elegance of your code.

"""
from .baseio import (IOClosed, StreamTextIOBase)
from .iterio import IteratorTextIO
from .csvio import (csv_text, CsvWriterTextIO, CsvDictWriterTextIO)
from .pipeio import PipeTextIO, pipe_text


__all__ = (
    'IOClosed',
    'StreamTextIOBase',
    'IteratorTextIO',
    'csv_text',
    'CsvWriterTextIO',
    'CsvDictWriterTextIO',
    'PipeTextIO',
    'pipe_text',
)


__version__ = '0.3.0'
