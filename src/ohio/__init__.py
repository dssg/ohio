"""I/O extras"""
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


__version__ = '0.2.0'
