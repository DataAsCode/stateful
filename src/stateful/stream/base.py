from datetime import timedelta
from itertools import repeat

import pandas as pd
from pandas.api.types import infer_dtype
import redblackpy as rb
import numpy as np


# from stateful.state_value import StateValue


class BaseStream:

    def __init__(self, configuration: dict = {}, dtype=None, history=None, index={}):
        """
        A Red-Black Tree written in C++ is used as the foundation, to make lookups efficient
        """
        self.dtype = dtype
        self.history = history
        self._iter = None
        self._index = index
        self.configuration = {"on_dublicate": "increment"}
        self.configuration.update(configuration if configuration else {})

    @property
    def interpolation(self):
        return self.configuration.get("interpolation", "floor")

    @property
    def extrapolation(self):
        return self.configuration.get("extrapolation", None if self.dtype in ["str", "object"] else 0)

    @property
    def arithmetic(self):
        return self.configuration.get("arithmetic", "union")

    @property
    def on_dublicate(self):
        return self.configuration["on_dublicate"]

    @property
    def start(self):
        return self.history.begin()

    @property
    def end(self):
        return self.history.end()

    @property
    def events(self):
        return self.history.values()

    def _infer_type(self, example):
        self.dtype = self.configuration.get("dtype", infer_dtype([example]))

        if self.configuration["on_dublicate"] == "keep":
            rb_dtype = "object"
        elif self.dtype == "string":
            rb_dtype = "str"
        elif self.dtype == "integer":
            rb_dtype = "float32"
        elif self.dtype == "floating":
            rb_dtype = "float32"
        else:
            rb_dtype = "object"

        self.history = rb.Series(dtype=rb_dtype,
                                 interpolate=self.interpolation,
                                 extrapolate=self.extrapolation,
                                 arithmetic=self.arithmetic)

    def _safe_add(self, date, state):
        self.history[date] = state

        # TODO Fix a bug in RedBlackPy which returns the wrong result when the query is in the index (DivisionByZero)
        if self.interpolation == "linear":
            self._index[date] = state

    def add(self, date, state):
        if self.dtype is None:
            self._infer_type(state)

        date = pd.to_datetime(date, utc=True)

        try:
            self._safe_add(date, state)
        except KeyError:
            if self.on_dublicate == "erase":
                self.history.erase(date)
                self._safe_add(date, state)
            elif self.on_dublicate == "keep":
                prev_state = self.history[date]
                self.history.erase(date)
                self._safe_add(date, [prev_state, state])
            elif self.on_dublicate == "increment":
                date = date + timedelta(seconds=1)
                self.add(date, state)

    def _safe_get(self, date):
        # TODO Fix a bug in RedBlackPy which returns the wrong result when the query is in the index (DivisionByZero)
        if self.interpolation == "linear":
            if date in self._index:
                result = self._index[date]
            else:
                result = self.history[date]
        else:
            result = self.history[date]

        if self.dtype == "integer":
            return int(result)
        else:
            return result

    def get(self, date_or_index):
        if isinstance(date_or_index, int) or isinstance(date_or_index, slice):
            return self._safe_get(self.history.index()[date_or_index])

        if len(self.history) == 0 or self.history.begin() > date_or_index:
            return None
        elif self.history.end() < date_or_index:
            return self._safe_get(self.history.end())
        else:
            return self._safe_get(date_or_index)

    def in_range(self, start, end, freq):
        if start < self.start:
            start_to_hist = zip(pd.date_range(start, self.start, freq), repeat(None))
        else:
            start_to_hist = []

        if end > self.end:
            end = self.history.end()
            hist_to_end = zip(pd.date_range(start, self.start, freq), repeat(end))
        else:
            hist_to_end = []

        return start_to_hist + self.history.uniform(start, end, pd.to_timedelta(freq)) + hist_to_end

    def iter(self, on=True):
        if on:
            self.history.on_itermode()
        else:
            self.history.off_itermode()

    def _merge_history(self, other, func):
        from stateful.stream.stream import Stream
        def safe_func(*args):
            try:
                return func(*args)
            except:
                return np.NaN

        index = list(rb.SeriesIterator([self.history, other.history])())
        values = []

        """
        We merge the two histories by only applying the function to the indexes which are within the range
        of each history
        """
        for idx in index:
            if (self.start <= idx <= self.end) and (other.start <= idx <= other.end):
                values.append(safe_func(self[idx], other[idx]))
            elif (self.start <= idx <= self.end):
                values.append(safe_func(self[idx], 0))
            else:
                values.append(safe_func(0, other[idx]))

        history = rb.Series(index,
                            values,
                            dtype=self.history.dtype,
                            interpolate=self.interpolation,
                            extrapolate=self.extrapolation,
                            arithmetic=self.arithmetic)

        return Stream(dtype=self.dtype, history=history, index=self._index)

    def _transform_history(self, func):
        from stateful.stream.stream import Stream
        return Stream(dtype=self.dtype, history=self.history.map(func), index=self._index)
