import pandas as pd
from stateful.representable import Representable


class EventBase(Representable):

    def items(self):
        raise NotImplementedError("items() should be implemented by all children")

    @property
    def value(self):
        raise NotImplementedError("this function must be implemented by all children")

    def apply(self, function):
        raise NotImplementedError("this function must be implemented by all children")

    def keys(self):
        raise NotImplementedError("this function must be implemented by all children")

    def isna(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __getitem__(self, item):
        raise NotImplementedError("this function must be implemented by all children")

    def __setitem__(self, key, value):
        raise NotImplementedError("this function must be implemented by all children")

    def __getattr__(self, item):
        raise NotImplementedError("this function must be implemented by all children")

    def __iter__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __contains__(self, item):
        raise NotImplementedError("this function must be implemented by all children")

    def __len__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def unwrap(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __add__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __radd__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __sub__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rsub__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __mul__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rmul__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __pow__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rpow__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __mod__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rmod__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __floordiv__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rfloordiv__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __truediv__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rtruediv__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __and__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __rand__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __or__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __ror__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __eq__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __neq__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __gt__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __ge__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __lt__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    def __le__(self, other):
        raise NotImplementedError("this function must be implemented by all children")

    """
    Unary operators
    """

    def __neg__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __pos__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __abs__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __invert__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __int__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __bool__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __float__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __str__(self):
        raise NotImplementedError("this function must be implemented by all children")

    def __repr__(self):
        raise NotImplementedError("this function must be implemented by all children")
