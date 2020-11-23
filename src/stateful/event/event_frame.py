from datetime import datetime
from typing import List, Dict
import numpy as np
import pandas as pd
from stateful.event.base import EventBase


class EventFrame(EventBase):

    def __init__(self, dates: np.ndarray, **kwargs):
        self.dates = dates
        self.length = len(dates)
        self._state: Dict[np.ndarray] = kwargs

    @property
    def value(self):
        if len(self._state) == 1:
            return self._state[list(self._state.keys())[0]]
        else:
            return self._state

    @property
    def empty(self):
        return bool(self._state)

    def add_column(self, name, array):
        if not isinstance(array, (np.ndarray, list, tuple)):
            array = self.empty_col(len(self), fill=array)
        assert len(array) == len(self.dates)

        self._state[name] = np.array(array)

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

        return self._state[item]

    def __iter__(self):
        if len(self._state) > 1:
            for i, date in enumerate(self.dates):
                yield date, {name: self._state[name][i] for name in self._state.keys()}
        else:
            for date, event in zip(self.dates, self._state[list(self._state.keys())[0]]):
                yield date, event

    def __iadd__(self, other):
        assert len(other.dates) == len(self.dates)
        self._state.update(other.data)
        return self

    def __add__(self, other):
        assert len(other.dates) == len(self.dates)
        return EventFrame(self.dates, **self._state, **other.data)

    def empty_col(self, size, fill=np.NaN):
        arr = np.empty(size)
        arr[:] = fill
        return arr

    def __or__(self, other):

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

        return EventFrame(dates, **data)

    def head(self, n=5) -> pd.DataFrame:
        return self.df().head()

    def df(self) -> pd.DataFrame:
        return pd.DataFrame(self._state)

    def __len__(self):
        return self.length
