from datetime import timedelta
from functools import partial

import numpy as np
import pandas as pd
from stateful.representable import Representable
from stateful.utils import list_of_instance


class BaseStream(Representable):

    def __init__(self, name, configuration: dict = None, dtype=None, history=None, index=None, streams=None,
                 function=None):
        Representable.__init__(self)
        self.name = name
        self.dtype = dtype
        self._history = history
        self._iter = None
        self._index = index if index is not None else {}
        self.configuration = {"on_dublicate": "increment"}
        self.configuration.update(configuration if configuration else {})
        self.streams = streams
        self.function = function

    @property
    def interpolation(self):
        return self.configuration.get("interpolation", "floor")

    @property
    def extrapolation(self):
        return self.configuration.get("extrapolation", None if self.dtype in ["str", "object"] else 0)

    @property
    def on_dublicate(self):
        return self.configuration["on_dublicate"]

    @property
    def start(self):
        raise NotImplementedError("start() should be implemented by all children")

    @property
    def end(self):
        raise NotImplementedError("end() should be implemented by all children")

    @property
    def first(self):
        return self._safe_get(self.start)

    @property
    def last(self):
        return self._safe_get(self.start)

    def head(self, n=5) -> pd.DataFrame:
        index, values = [], []
        iterator = iter(self)

        while len(index) < n:
            idx, value = next(iterator)
            index.append(idx)
            values.append(value)

        if list_of_instance(values, dict):
            return pd.DataFrame(values, index=index)
        else:
            return pd.DataFrame(columns={self.name: values}, index=index)

    def df(self) -> pd.DataFrame:
        index, values = [], []
        for idx, value in self:
            index.append(idx)
            values.append(value)

        if list_of_instance(values, dict):
            return pd.DataFrame(values, index=index)
        else:
            return pd.DataFrame(columns={self.name: values}, index=index)

    def _safe_get_before(self, date):
        if self.interpolation == "ceil":
            return self.first

        if self.dtype in ["integer", "floating"]:
            return 0

        return np.NaN

    def _safe_get(self, date):
        raise NotImplementedError("_safe_get() should be implemented by all children")

    def _safe_get_after(self, date):
        if self.interpolation == "floor":
            return self.last

        if self.dtype in ["integer", "floating"]:
            return 0

        return np.NaN

    def alias(self, name):
        raise NotImplementedError("alias(name) should be implemented by all children")

    def _safe_add(self, date, state):
        raise NotImplementedError("_safe_add() should be implemented by all children")

    def values(self) -> set:
        raise NotImplementedError("values() should be implemented by all children")

    def keys(self) -> set:
        raise NotImplementedError("keys() should be implemented by all children")

    def on(self, on=True) -> None:
        raise NotImplementedError("on() should be implemented by all children")

    def iter(self):
        raise NotImplementedError("iter() should be implemented by all children")

    def within(self, date) -> bool:
        raise NotImplementedError("within(date) should be implemented by all children")

    def cast(self, value, inverse=False):
        if pd.isna(value):
            return value

        if self.dtype == "integer":
            return int(value) if not inverse else int(value)
        elif self.dtype == "boolean":
            return bool(value) if not inverse else int(value)
        else:
            return value

    def get(self, date):
        if isinstance(date, list) or isinstance(date, tuple) or isinstance(date, set):
            return [self.get(d) for d in date]

        if not self:
            return np.NaN

        date = pd.to_datetime(date, utc=True)

        if self.start > date:
            return self.cast(self._safe_get_before(date))
        elif self.end < date:
            return self.cast(self._safe_get_after(date))
        else:
            return self.cast(self._safe_get(date))

    def add(self, date, state):
        date = pd.to_datetime(date, utc=True)
        state = self.cast(state, inverse=True)

        try:
            self._safe_add(date, state)
        except KeyError:
            if self.on_dublicate == "increment":
                date = date + timedelta(seconds=1)
                self.add(date, state)

    """
    List methods
    """

    def __len__(self):
        raise NotImplementedError("__len__ should be implemented by all children")

    def __contains__(self, item):
        try:
            date = pd.to_datetime(item, utc=True)
            return self.start <= date <= self.end
        except:
            return item in self.values()

    def __iter__(self):
        self.on(True)
        self._iter = self.iter()
        return self

    def __next__(self):
        try:
            idx = next(self._iter)
            return idx, self.get(idx)
        except StopIteration:
            self.on(False)
            raise StopIteration

    """
    Add dictionary methods
    """

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, date, state):
        type_check = all([
            isinstance(date, list) or isinstance(date, tuple) or isinstance(date, np.ndarray),
            isinstance(state, list) or isinstance(state, tuple) or isinstance(state, np.ndarray)
        ])

        if type_check:
            for d, s in zip(date, state):
                self.add(d, s)
        else:
            self.add(date, state)

    """
    Add merge functions
    """

    @staticmethod
    def _merge(left, right, func):
        from stateful.stream.stream_multi import MultiStream
        return MultiStream(name=f"{left.name}_{right.name}",
                           configuration=left,
                           dtype=left.dtype,
                           streams=[left, right],
                           function=func)

    def _transform(self, func):
        from stateful.stream.stream_aggregated import AggregatedStream
        return AggregatedStream(name=self.name,
                                configuration=self.configuration,
                                dtype=self.dtype,
                                stream=self,
                                function=func)

    def apply(self, func):
        return self._transform(func)

    """
    Add Destructor
    """

    def __del__(self):
        del self.name
        del self.dtype
        del self._history
        del self._iter
        del self._index
        del self.configuration
        del self.streams
        del self.function

    """
    Add mathematical methods
    """

    @staticmethod
    def _generate_func(stream_a, stream_b, func):
        name_a = stream_a.name
        name_b = stream_b.name
        return partial(lambda state, a, b: func(state[a], state[b]), a=name_a, b=name_b)

    @staticmethod
    def _ensure_unique_naming(stream_left, stream_right):
        if stream_left.name != stream_right.name:
            return stream_left, stream_right

        return stream_left.alias(f"{stream_left.name}_left"), stream_right.alias(f"{stream_right.name}_right")

    def __add__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a + b))
        else:
            return self._transform(lambda a: a + other)

    def __radd__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b + a))
        else:
            return self._transform(lambda a: other + a)

    def __sub__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a - b))
        else:
            return self._transform(lambda a: a - other)

    def __rsub__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b - a))
        else:
            return self._transform(lambda a: other - a)

    def __mul__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a * b))
        else:
            return self._transform(lambda a: a * other)

    def __rmul__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b * a))
        else:
            return self._transform(lambda a: other * a)

    def __pow__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(self, other, lambda a, b: a ** b))
        else:
            return self._transform(lambda a: a ** other)

    def __rpow__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b ** a))
        else:
            return self._transform(lambda a: other ** a)

    def __mod__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a % b))
        else:
            return self._transform(lambda a: a % other)

    def __rmod__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b % a))
        else:
            return self._transform(lambda a: other % a)

    def __floordiv__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a // b))
        else:
            return self._transform(lambda a: a // other)

    def __rfloordiv__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(self, other, lambda a, b: b // a))
        else:
            return self._transform(lambda a: other // a)

    def __truediv__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a / b))
        else:
            return self._transform(lambda a: a / other)

    def __rtruediv__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b / a))
        else:
            return self._transform(lambda a: other / a)

    def __and__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a and b))
        else:
            return self._transform(lambda a: a and other)

    def __rand__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b and a))
        else:
            return self._transform(lambda a: other and a)

    def __or__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a or b))
        else:
            return self._transform(lambda a: a or other)

    def __ror__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b or a))
        else:
            return self._transform(lambda a: other or a)

    def __eq__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: a == b))
        else:
            return self._transform(lambda a: a == other)

    def __neq__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b != a))
        else:
            return self._transform(lambda a: other != a)

    def __gt__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b > a))
        else:
            return self._transform(lambda a: other > a)

    def __ge__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b >= a))
        else:
            return self._transform(lambda a: other >= a)

    def __lt__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b < a))
        else:
            return self._transform(lambda a: other < a)

    def __le__(self, other):
        if issubclass(type(other), BaseStream):
            left, right = self._ensure_unique_naming(self, other)
            return self._merge(left, right, self._generate_func(left, right, lambda a, b: b <= a))
        else:
            return self._transform(lambda a: other <= a)

    """
    Unary operators
    """

    def __neg__(self):
        return self._transform(lambda a: a.__neq__())

    def __pos__(self):
        return self._transform(lambda a: a.__pos__())

    def __abs__(self):
        return self._transform(lambda a: a.__abs__())

    def __invert__(self):
        return self._transform(lambda a: a.__invert__())

    def __int__(self):
        return self._transform(lambda a: a.__int__())

    def __bool__(self):
        return bool(len(self))

    def __long__(self):
        return self._transform(lambda a: a.__long__())

    def __float__(self):
        return self._transform(lambda a: a.__float__())

    def __complex__(self):
        return self._transform(lambda a: a.__complex__())

    def __oct__(self):
        return self._transform(lambda a: a.__oct__())

    def __hex__(self):
        return self._transform(lambda a: a.__hex__())
