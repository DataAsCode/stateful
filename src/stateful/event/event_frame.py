from datetime import datetime
from typing import List, Dict
import numpy as np
import pandas as pd
from pandas import DatetimeIndex
from stateful.event.base import EventBase
from stateful.event.event import Event
from stateful.event.event_column import EventColumn
from stateful.utils import list_of_instance


class EventFrame(EventBase):

    def __init__(self, dates: DatetimeIndex, kwargs=None):
        self.dates = dates
        self.length = len(dates)
        self._state: Dict[np.ndarray] = kwargs if kwargs else {}

    @property
    def value(self):
        if len(self._state) == 1:
            return self._state[list(self._state.keys())[0]]
        else:
            return self._state

    @property
    def empty(self):
        return not bool(self._state)

    def items(self):
        return self._state.items()

    def add_column(self, other: EventColumn):
        assert isinstance(other, EventColumn)
        assert (self.dates == other.dates).all()
        self._state[other.name] = other
        return self

    def transform(self, name, event_func, with_date=True):
        if with_date:
            events = np.array([event_func(date, event) for date, event in self])
        else:
            events = np.array([event_func(event) for _, event in self])
        return EventFrame(self.dates, **{str(name): events})

    def __getitem__(self, item):
        if isinstance(item, datetime):
            i = self.dates[self.dates == item][0]
            return {name: self._state[name][i] for name in self._state.keys()}
        if list_of_instance(item, str):
            return EventFrame(self.dates, {name: self._state[name] for name in item})

        return self._state[item]

    def __iter__(self):
        for i, date in enumerate(self.dates):
            yield Event(date, {name: self._state[name][i] for name in self._state.keys()})

    def __iadd__(self, other):
        assert len(other.dates) == len(self.dates)
        self._state.update(other.data)
        return self

    def __add__(self, other):
        assert len(other.dates) == len(self.dates)
        return EventFrame(self.dates, **self._state, **other.data)

    def empty_col(self, name, size, fill=np.NaN):
        arr = np.empty(size)
        arr[:] = fill
        return EventColumn(name, self.dates, arr)

    def __or__(self, other):
        return self.concat(other)

    def concat(self, other):
        dates = np.concatenate([self.dates, other.dates], axis=None)
        data = {}
        for key in set(self._state.keys()) | set(other.data.keys()):
            own_data = self._state.get(key)
            if not own_data:
                own_data = self.empty_col(len(self))

            other_data = other.data.get(key)
            if not other_data:
                other_data = self.empty_col(len(other))

            data[key] = np.concatenate([own_data, other_data], axis=None)

        return EventFrame(dates, data)

    def head(self, n=5) -> pd.DataFrame:
        return self.df()

    def df(self) -> pd.DataFrame:
        return pd.DataFrame({column.name: column.events for column in self._state.values()}, index=self.dates)

    def __len__(self):
        return self.length

    def apply(self, function, name=None, vectorized=False, ):
        if vectorized:
            events = function(self)
        else:
            events = np.array(list(map(function, list(self))))

        name = name if name else "value"
        return EventColumn(name, self.dates, events=events)

    def keys(self):
        return self._state.keys()

    def isna(self):
        pass

    def __setitem__(self, key, value):
        pass

    def __getattr__(self, item):
        pass

    def __contains__(self, item):
        return item in self._state

    def unwrap(self):
        pass

    def __radd__(self, other):
        pass

    def __sub__(self, other):
        pass

    def __rsub__(self, other):
        pass

    def __mul__(self, other):
        pass

    def __rmul__(self, other):
        pass

    def __pow__(self, other):
        pass

    def __rpow__(self, other):
        pass

    def __mod__(self, other):
        pass

    def __rmod__(self, other):
        pass

    def __floordiv__(self, other):
        pass

    def __rfloordiv__(self, other):
        pass

    def __truediv__(self, other):
        pass

    def __rtruediv__(self, other):
        pass

    def __and__(self, other):
        pass

    def __rand__(self, other):
        pass

    def __ror__(self, other):
        pass

    def __eq__(self, other):
        pass

    def __neq__(self, other):
        pass

    def __gt__(self, other):
        pass

    def __ge__(self, other):
        pass

    def __lt__(self, other):
        pass

    def __le__(self, other):
        pass

    def __neg__(self):
        pass

    def __pos__(self):
        pass

    def __abs__(self):
        pass

    def __invert__(self):
        pass

    def __int__(self):
        pass

    def __bool__(self):
        pass

    def __float__(self):
        pass

    def __str__(self):
        return f"EventFrame({self.keys()})"
