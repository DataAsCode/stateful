from functools import partial
from typing import List

from stateful.representable import Representable
import pandas as pd
from collections import defaultdict
import numpy as np


class TransposedFunction:
    def __init__(self, name, space_function, column_function):
        self.storage = defaultdict(list)
        self.name = name
        self.space_function = space_function
        self.final = column_function

    def __call__(self, events, dates):
        for date, result in self.space_function(events, dates):
            self.storage[date].append(result)

    def column(self):
        result = self.final(self.storage)

        self.storage = defaultdict(list)

        return result


class TransposedState(Representable):

    def __init__(self, state, freq):
        self._index = pd.date_range(state.start, state.end, freq=freq)
        self._df = pd.DataFrame()
        self._state = state
        self._state_functions: List[TransposedFunction] = []

    def compute(self):
        self._df = pd.DataFrame(index=self._index)
        for events in self._state.all(self._index):
            for state_function in self._state_functions:
                state_function(events)

        for state_function in self._state_functions:
            self._df[state_function.name] = state_function.column()

        return self

    @property
    def empty(self):
        return len(self._df) == 0

    def head(self, n=5) -> pd.DataFrame:
        return self._df.head(n)

    def df(self) -> pd.DataFrame:
        return self._df

    def count(self, column, value, name="COUNT"):
        def _count(frame, column, value):
            return zip(frame.dates, frame[column] == value)

        def final(date_aggregate):
            return [sum(values) for values in date_aggregate.values()]

        self._state_functions.append(TransposedFunction(name,
                                                        space_function=partial(_count, column=column, value=value),
                                                        column_function=final))

        return self
