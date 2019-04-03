import abc
import contextlib
import functools


class Wrapper(abc.ABC):
    """Functional wrapping via class definition and instantiation.

    Provides ``repr`` which indicates the wrapping chain.

    """
    def __init__(self, func):
        functools.update_wrapper(self, func)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__wrapped__})"

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        return self.__wrapped__(*args, **kwargs)


def wrapper(func):
    """Define the decorated function as a function ``Wrapper``, which
    decorates other functions, and receives an argument reference to the
    decorated function upon invocation.

    In place of::

        def assert_result(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                result = func(*args, **kwargs)
                assert result
                return result

            return wrapped

    We might instead::

        @wrapper
        def assert_result(func, *args, **kwargs):
            result = func(*args, **kwargs)
            assert result
            return result

    Functions decorated by ``wrapper`` are subclasses of ``Wrapper``.

    """
    def call(self, *args, **kwargs):
        return func(self.__wrapped__, *args, **kwargs)

    return type(func.__name__, (Wrapper,), {
        '__call__': call,
        '__module__': func.__module__,
    })


# contextlib improvements #

class _SharingMixin:

    class ContextWrapped(Wrapper):

        class BoundWrapper:

            def __init__(self, wrapped, instance):
                self.__func__ = wrapped
                self.__self__ = instance

            def __call__(self, *args, **kwargs):
                return self.__func__._call_(self.__self__, *args, **kwargs)

        def __init__(self, cm, func):
            super().__init__(func)
            self.cm = cm

        def __get__(self, instance, cls=None):
            if instance is not None:
                return self.BoundWrapper(self, instance)

            return self

        def _call_(self, instance, *args, **kwargs):
            # unlike the builtin, we'll pass the result of __enter__ to the
            # decorated function as an initial argument
            with self.cm._recreate_cm() as ctx:
                ctx_args = () if ctx is None else (ctx,)
                inner_self = () if instance is None else (instance,)
                return self.__wrapped__(*(inner_self + ctx_args + args), **kwargs)

        def __call__(self, *args, **kwargs):
            return self._call_(None, *args, **kwargs)

        def __repr__(self):
            return f"{self.__class__.__name__}({self.cm}, {self.__wrapped__})"

    def __call__(self, func):
        return self.ContextWrapped(self, func)


class _ComposingMixin:

    def _get_cm(self):
        return self

    @property
    def _wrapper(self):
        return contextmanager

    def manager(self, func):
        """construct contextmanager that requires (composes) another."""
        inner = func if isinstance(func, self._wrapper) else self._wrapper(func)

        @functools.wraps(func)
        def composition(*args, **kwds):
            with self._get_cm() as ctx0:
                ctx_args = () if ctx0 is None else (ctx0,)
                with inner(*(ctx_args + args), **kwds) as ctx1:
                    yield ctx1

        return self._wrapper(composition)


class SharingContextDecorator(_SharingMixin, _ComposingMixin, contextlib.ContextDecorator):
    """ContextDecorator which shares its context with the decorated
    function and permits extension via decorator method ``manager``.

    Like ``contextmanager``, but does not wrap simple generators;
    declaration of the context manager interface -- ``__enter__`` and
    ``__exit__`` -- is required.

    """


# better contextmanager

class _SharingGeneratorContextManager(_SharingMixin, contextlib._GeneratorContextManager):

    def __repr__(self):
        return f"contextmanager({self.func})"


class contextmanager(_ComposingMixin, Wrapper):
    """@contextmanager decorator.

    Unlike the built-in provided by ``contextlib``:

    1. Context values provided by manager generators are provided to
       decorated functions upon invocation.

    2. Context decorators which require no decoration arguments may be
       used as decorators without superfluous invocation (empty
       parentheses).

    3. Context managers may be composed, such that the decorated
       generator extends the decorating manager, (and receives its
       context).

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

    Moreover, (1) and (2):

        @some_generator
        def my_func(value):
            ...

    ``my_func`` may be invoked without arguments. The ``some_generator``
    context will be recreated each time, and its value(s) provided to
    the function, (seamlessly).

    Nested contexts may also be recreated through decoration (3):

        @some_generator.manager
        def another_generator(value):
            <setup>
            try:
                yield <value1>
            finally:
                <cleanup>

        @another_generator
        def my_func1(value1):
            ...

    """
    def _get_cm(self):
        return self()

    def __call__(self, *args, **kwds):
        # unlike the builtin, we'll enable decoration without
        # invocation of the decorator
        if not kwds and len(args) == 1 and callable(args[0]):
            return _SharingGeneratorContextManager(self.__wrapped__, (), kwds)(args[0])

        return _SharingGeneratorContextManager(self.__wrapped__, args, kwds)
