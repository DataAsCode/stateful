from stateful.event.base import EventBase

import pandas as pd
import numpy as np
from stateful.utils import list_of_instance


class Event(EventBase):
    def __init__(self, date, state=None):
        self.date = date
        self._state = state if state else {}

    @property
    def value(self):
        if isinstance(self._state, dict):
            if len(self._state) == 1:
                return self._state[list(self._state.keys())[0]]
            else:
                return self._state
        else:
            return self._state

    def apply(self, function, name=None, vectorized=False):
        new_state = {"value": function(self)}
        return Event(self.date, new_state)

    def keys(self):
        return self._state.keys()

    def items(self):
        return self._state.items()

    def isna(self):
        return pd.isna(self.value)

    def __getitem__(self, item):
        if isinstance(self._state, dict):
            if isinstance(item, str):
                return self._state.get(item, np.NaN)
            if list_of_instance(item, str):
                return Event(date=self.date, state={name: self[name] for name in item})
        else:
            raise AttributeError(f"{self.value} is not a dictionary")

    def __setitem__(self, key, value):
        self._state[key] = value

    def __getattr__(self, item):
        try:
            self.__getattribute__(item)
        except:
            return self[item]

    def __iter__(self):
        if isinstance(self.value, dict):
            return iter(self.value)
        else:
            raise AttributeError(f"{self.value} not iterable")

    def __contains__(self, item):
        if isinstance(self.value, dict):
            return item in self.value
        else:
            raise AttributeError(f"{self.value} not cannot contain other variables")

    def __len__(self):
        if isinstance(self.value, dict):
            return len(self.value)
        else:
            raise AttributeError(f"{self.value} has no length")

    def unwrap(self):
        if isinstance(self.value, dict):
            data = {}
            for key, snap in self.value.items():
                try:
                    data[key] = snap.unwrap()
                except:
                    data[key] = snap
            return data
        else:
            try:
                return self.value.unwrap()
            except:
                return self.value

    def __add__(self, other):
        try:
            return self.value + other
        except:
            return self.value + other.value

    def __radd__(self, other):
        try:
            return other + self.value
        except:
            return other.value + self.value

    def __sub__(self, other):
        try:
            return self.value - other
        except:
            return self.value - other.value

    def __rsub__(self, other):
        try:
            return other - self.value
        except:
            return other.value - self.value

    def __mul__(self, other):
        try:
            return self.value * other
        except:
            return self.value * other.value

    def __rmul__(self, other):
        try:
            return other * self.value
        except:
            return other.value * self.value

    def __pow__(self, other):
        try:
            return self.value ** other
        except:
            return self.value ** other.value

    def __rpow__(self, other):
        try:
            return other ** self.value
        except:
            return other.value ** self.value

    def __mod__(self, other):
        try:
            return self.value % other
        except:
            return self.value % other.value

    def __rmod__(self, other):
        try:
            return other % self.value
        except:
            return other.value % self.value

    def __floordiv__(self, other):
        try:
            return self.value // other
        except:
            return self.value // other.value

    def __rfloordiv__(self, other):
        try:
            return other // self.value
        except:
            return other.value // self.value

    def __truediv__(self, other):
        try:
            return self.value / other
        except:
            return self.value / other.value

    def __rtruediv__(self, other):
        try:
            return other / self.value
        except:
            return other.value / self.value

    def __and__(self, other):
        try:
            return self.value and other
        except:
            return self.value and other.value

    def __rand__(self, other):
        try:
            return other and self.value
        except:
            return other.value and self.value

    def __or__(self, other):
        try:
            return self.value or other
        except:
            return self.value or other.value

    def __ror__(self, other):
        try:
            return other or self.value
        except:
            return other.value or self.value

    def __eq__(self, other):
        try:
            return self.value == other
        except:
            return self.value == other.value

    def __neq__(self, other):
        try:
            return self.value != other
        except:
            return self.value != other.value

    def __gt__(self, other):
        try:
            return self.value > other
        except:
            return self.value > other.value

    def __ge__(self, other):
        try:
            return self.value >= other
        except:
            return self.value >= other.value

    def __lt__(self, other):
        try:
            return self.value < other
        except:
            return self.value < other.value

    def __le__(self, other):
        try:
            return self.value <= other
        except:
            return self.value <= other.value

    """
    Unary operators
    """

    def __neg__(self):
        return self.value.__neq__() if not pd.isna(self.value) else self.value

    def __pos__(self):
        return self.value.__pos__() if not pd.isna(self.value) else self.value

    def __abs__(self):
        return abs(self.value) if not pd.isna(self.value) else self.value

    def __invert__(self):
        return self.value.__invert__() if not pd.isna(self.value) else self.value

    def __int__(self):
        return int(self.value) if not pd.isna(self.value) else self.value

    def __bool__(self):
        return bool(self.value) if not pd.isna(self.value) else self.value

    def __long__(self):
        return self.value.__long__() if not pd.isna(self.value) else self.value

    def __float__(self):
        return float(self.value) if not pd.isna(self.value) else self.value

    def __str__(self):
        return str(self.value) if not pd.isna(self.value) else self.value

    def __repr__(self):
        return f"{type(self).__name__}({repr(self.value)})"
