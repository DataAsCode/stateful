from datetime import datetime
from functools import partial
import numpy as np

from stateful.stream.base import BaseStream


class StreamWithMagic(BaseStream):
    """
    Add magic methods to the stream

    """

    """
    General func
    """

    def __repr__(self):
        return repr(self.history)

    """
    Type conversion
    """

    def __bool__(self):
        return bool(len(self))

    """
    List methods
    """

    def __len__(self):
        return len(self.history)

    def __contains__(self, item):
        if isinstance(item, datetime):
            return self.start <= item <= self.end

        return item in set(self.history.values())

    def __iter__(self):
        self.history.on_itermode()
        self._iter = self.history.iteritems()
        return self

    def __next__(self):
        try:
            return next(self._iter)
        except StopIteration:
            self.history.off_itermode()
            raise StopIteration

    """
    Add dictionary methods
    """

    def __getitem__(self, item):
        if isinstance(item, list) or isinstance(item, tuple):
            return [self.get(date) for date in item]

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
    Add mathematical methods
    """

    def __add__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a + b)
        else:
            return self._transform_history(lambda a: a + other)

    def __radd__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b + a)
        else:
            return self._transform_history(lambda a: other + a)

    def __sub__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a - b)
        else:
            return self._transform_history(lambda a: a - other)

    def __rsub__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b - a)
        else:
            return self._transform_history(lambda a: other - a)

    def __mul__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a * b)
        else:
            return self._transform_history(lambda a: a * other)

    def __rmul__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b * a)
        else:
            return self._transform_history(lambda a: other * a)

    def __div__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a // b)
        else:
            return self._transform_history(lambda a: a // other)

    def __rdiv__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b // a)
        else:
            return self._transform_history(lambda a: other // a)

    def __truediv__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a / b)
        else:
            return self._transform_history(lambda a: a / other)

    def __rtruediv__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b / a)
        else:
            return self._transform_history(lambda a: other / a)

    def __and__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a and b)
        else:
            return self._transform_history(lambda a: a and other)

    def __rand__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b and a)
        else:
            return self._transform_history(lambda a: other and a)

    def __or__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: a or b)
        else:
            return self._transform_history(lambda a: a or other)

    def __ror__(self, other):
        from stateful.stream import Stream

        if isinstance(other, Stream):
            return self._merge_history(other, lambda a, b: b or a)
        else:
            return self._transform_history(lambda a: other or a)
