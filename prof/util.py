import collections
import contextlib
import functools


def histogram(stream):
    counts = collections.defaultdict(int)
    for item in stream:
        counts[item] += 1
    return counts


# contextlib improvements #

class _SharingMixin:

    def __call__(self, func):
        @functools.wraps(func)
        def inner(*args, **kwds):
            # unlike the builtin, we'll pass the result of __enter__ to the
            # decorated function as an initial argument
            with self._recreate_cm() as ctx:
                ctx_args = () if ctx is None else (ctx,)
                return func(*(ctx_args + args), **kwds)
        return inner


class _ComposingMixin:

    def _get_cm(self):
        return self

    def _wrap(self, func):
        return contextmanager(func)

    def manager(self, func):
        """construct contextmanager that requires (composes) another."""
        inner = self._wrap(func)

        @functools.wraps(func)
        def composition(*args, **kwds):
            with self._get_cm() as ctx0:
                with inner(ctx0) as ctx1:
                    yield ctx1

        return self._wrap(composition)


class SharingContextDecorator(_SharingMixin, _ComposingMixin, contextlib.ContextDecorator):
    pass


# better contextmanager

class _SharingGeneratorContextManager(_SharingMixin, contextlib._GeneratorContextManager):
    pass


class Wrapper:

    def __init__(self, func):
        functools.update_wrapper(self, func)


class contextmanager(_ComposingMixin, Wrapper):
    """@contextmanager decorator.

    Typical usage:

        @contextmanager
        def some_generator(<arguments>):
            <setup>
            try:
                yield <value>
            finally:
                <cleanup>

    This makes this:

        with some_generator(<arguments>) as <variable>:
            <body>

    equivalent to this:

        <setup>
        try:
            <variable> = <value>
            <body>
        finally:
            <cleanup>

    """
    def _get_cm(self):
        return self()

    def __call__(self, *args, **kwds):
        # unlike the builtin, we'll enable decoration without
        # invocation of the decorator
        if not kwds and len(args) == 1 and callable(args[0]):
            return _SharingGeneratorContextManager(self.__wrapped__, (), kwds)(args[0])
        return _SharingGeneratorContextManager(self.__wrapped__, args, kwds)
