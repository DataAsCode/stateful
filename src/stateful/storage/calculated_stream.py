from functools import partial
from typing import Optional, Callable, List

import numpy as np
import pandas as pd
from stateful.representable import Representable
from stateful.utils import cast_output
from stateful.space import Space


class CalculatedStream(Representable):

    def __init__(self, dependencies: List[str],
                 dtype: Optional[str] = None,
                 function: Optional[Callable] = None,
                 parent: Optional[Space] = None,
                 vectorized=False):

        self.dependencies = dependencies
        self.function = function
        self._parent: Optional[Space] = parent
        self._dtype = dtype
        self.vectorized = vectorized

    @property
    def dtype(self):
        if not self._dtype and len(self.dependencies) == 1 and self._parent:
            self._dtype = self._stream.dtype

        return self._dtype

    @property
    def _controller(self):
        return self._parent.controller

    @property
    def _stream(self):
        assert len(self.dependencies) == 1, "Cannot compute this on multi streams"
        return self._controller[self.dependencies[0]]

    @property
    def start(self):
        assert self._parent, "cannot call start on unattached CalculatedStream"
        return min([self._controller[name].start for name in self.dependencies if self._controller[name].start])

    @property
    def end(self):
        assert self._parent, "cannot call end on unattached CalculatedStream"
        return max([self._controller[name].end for name in self.dependencies if self._controller[name].end])

    @property
    def first(self):
        assert self._parent, "cannot call first on unattached CalculatedStream"
        return self.get(self.start)

    @property
    def last(self):
        assert self._parent, "cannot call last on unattached CalculatedStream"
        return self.get(self.end)

    @property
    def empty(self):
        if self._parent is None:
            return True

        return all([self._controller[name].empty for name in self.dependencies])

    @property
    def configuration(self):
        assert self._parent, "cannot call last on unattached CalculatedStream"
        return self._stream.configuration

    @property
    def interpolation(self):
        assert self._parent, "cannot call last on unattached CalculatedStream"
        return self._stream.interpolation

    def assign_to(self, parent):
        return CalculatedStream(self.dependencies, dtype=self.dtype, function=self.function, parent=parent)

    def add(self, date, value):
        assert len(self.dependencies) == 1
        self._controller[self.dependencies[0]].add(date, value)

    def get(self, date, cast=True):
        if self.function:
            event = self._controller.get(date, self.dependencies, cast=False)
            event = event.apply(self.function)
            return cast_output(self.dtype, event) if cast else event

        return self._controller.get(date, self.dependencies, cast=cast)

    def head(self, n=5) -> pd.DataFrame:
        return self._parent.head(n)

    def df(self) -> pd.DataFrame:
        return self._parent.df()

    @staticmethod
    def _generate_func(stream_a, stream_b, func):
        name_a = stream_a.name
        name_b = stream_b.name
        return partial(lambda state, a, b: func(state[a], state[b]), a=name_a, b=name_b)

    def apply(self, function, vectorized=False):
        def maybe_wrap(func, old_func):
            if old_func is None:
                return func

            def wrapper(*args, **kwargs):
                return func(old_func(*args, **kwargs))

            return wrapper

        return CalculatedStream(self.dependencies,
                                function=maybe_wrap(function, self.function),
                                dtype=self.dtype,
                                vectorized=vectorized,
                                parent=self._parent)

    def __assertions(self, other):
        # @no:format
        assert len(self.dependencies) == len(other.dependencies) == 1, "Operators only works on single streams"
        assert self._parent is None or other._parent is None or self._parent == other._parent, "Cannot do operators across states and spaces"
        # @do:format

    def __getitem__(self, item):
        return self.get(item)

    def calculate(self, event, name):
        return event.apply(self.function, name=name, vectorized=self.vectorized)

    def _calculate_dtype(self, other):
        dtypes = {self.dtype, other.dtype}

        if "object" in dtypes:
            return "object"
        if "string" in dtypes:
            return "string"
        if "floating" in dtypes:
            return "floating"
        if "integer" in dtypes:
            return "integer"
        if "boolean" in dtypes:
            return "boolean"
        else:
            return None

    def __add__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] + state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] + right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __radd__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] + state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right + state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __sub__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] - state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] - right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rsub__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] - state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right - state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __mul__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] * state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] * right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rmul__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] * state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right * state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __pow__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] ** state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] ** right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rpow__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] ** state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right ** state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __mod__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] % state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] % right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rmod__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] % state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right % state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __floordiv__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] // state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] // right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rfloordiv__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] // state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right // state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __truediv__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] / state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] / right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rtruediv__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[right] / state[left],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: right / state[left], left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __and__(self, other):
        if isinstance(other, CalculatedStream):
            def two_way_and(left, right):
                try:
                    return left and right
                except:
                    return left & right

            self.__assertions(other)
            function = partial(lambda state, left, right: two_way_and(state[left], state[right]),
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: two_way_and(state[left], right), left=self.dependencies[0],
                               right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __rand__(self, other):
        return self.__and__(other)

    def __or__(self, other):
        if isinstance(other, CalculatedStream):
            def two_way_or(left, right):
                try:
                    return left or right
                except:
                    return left | right

            self.__assertions(other)
            function = partial(lambda state, left, right: two_way_or(state[left], state[right]),
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: two_way_or(state[left], right), left=self.dependencies[0],
                               right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __ror__(self, other):
        return self.__or__(other)

    def __eq__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] == state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] == right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __neq__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] != state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] != right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __gt__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] > state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] > right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __ge__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] >= state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] >= right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __lt__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] < state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] < right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    def __le__(self, other):
        if isinstance(other, CalculatedStream):
            self.__assertions(other)
            function = partial(lambda state, left, right: state[left] <= state[right],
                               left=self.dependencies[0],
                               right=other.dependencies[0])
            return CalculatedStream(dependencies=self.dependencies + other.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)
        else:
            function = partial(lambda state, left, right: state[left] <= right, left=self.dependencies[0], right=other)
            return CalculatedStream(dependencies=self.dependencies,
                                    function=function,
                                    dtype=self._calculate_dtype(other),
                                    parent=self._parent)

    """
    Unary operators
    """

    def __neg__(self):
        assert len(self.dependencies) == 1, "can only negate single streams"

        def function(event, key):
            return event[key].__neg__()

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __pos__(self):
        assert len(self.dependencies) == 1, "__pos__ only works on single streams"

        def function(event, key):
            return event[key].__pos__()

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __abs__(self):
        assert len(self.dependencies) == 1, "__abs__ only works on single streams"

        def function(event, key):
            return event[key].__abs__()

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __invert__(self):
        assert len(self.dependencies) == 1, "__invert__ only works on single streams"

        def function(event, key):
            return event[key].__invert__()

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __int__(self):
        assert len(self.dependencies) == 1, "__int__ only works on single streams"

        def function(event, key):
            try:
                return int(event[key])
            except:
                return event[key].astype(int)

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __bool__(self):
        assert len(self.dependencies) == 1, "__bool__ only works on single streams"

        def function(event, key):
            try:
                return bool(event[key])
            except:
                return event[key].astype(bool)

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __float__(self):
        assert len(self.dependencies) == 1, "__float__ only works on single streams"

        def function(event, key):
            try:
                return bool(event[key])
            except:
                return event[key].astype(float)

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)

    def __hash__(self):
        assert len(self.dependencies) == 1, "__hex__ only works on single streams"

        def function(event, key):
            try:
                return hash(event[key])
            except:
                return np.array([hash(v) for v in event[key]], dtype="object")

        return self.apply(partial(function, key=self.dependencies[0]), vectorized=True)
