"""I/O extras"""
from .baseio import (IOClosed, StreamTextIOBase)
from .iterio import IteratorTextIO
from .csvio import (csv_text, CsvWriterTextIO, CsvDictWriterTextIO)
from .pipeio import PipeTextIO


__all__ = (
    'IOClosed',
    'StreamTextIOBase',
    'IteratorTextIO',
    'csv_text',
    'CsvWriterTextIO',
    'CsvDictWriterTextIO',
    'PipeTextIO',
)


__version__ = '0.1.2'
