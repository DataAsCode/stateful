from stateful.stream.base import BaseStream
import redblackpy as rb
from pandas.api.types import infer_dtype
import numpy as np

class Stream(BaseStream):

    def __init__(self, name, configuration: dict = None, dtype=None, history=None, index=None):
        BaseStream.__init__(self, name, configuration, dtype, history=history, index=index)

    @property
    def start(self):
        return self.history.begin() if self.history else None

    @property
    def end(self):
        return self.history.end() if self.history else None

    @property
    def history(self):
        return self._history

    def alias(self, name):
        return Stream(name, self.configuration, self.dtype, self.history, self._index)

    def _safe_get(self, date):
        if not self:
            return np.NaN

        # TODO Fix a bug in RedBlackPy which returns the wrong result when the query is in the index (DivisionByZero)
        if self.history.interpolation == "linear":
            if date in self._index:
                return self._index[date]
            else:
                return self.history[date]
        else:
            return self.history[date]

    def within(self, date) -> bool:
        return self.start <= date <= self.end

    def keys(self):
        if self.history:
            return set(self.history.index())
        return set()

    def values(self) -> set:
        if self.history:
            return set(self.history.values())
        return set()

    def on(self, on=True) -> None:
        if self.history is not None:
            if on:
                self.history.on_itermode()
            else:
                self.history.off_itermode()

    def __len__(self):
        if self.history is not None:
            return len(self.history)
        return 0

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
        elif self.dtype == "boolean":
            rb_dtype = "uint8"
        else:
            rb_dtype = "object"

        self._history = rb.Series(name=self.name,
                                  dtype=rb_dtype,
                                  interpolate=self.interpolation,
                                  extrapolate=self.extrapolation)

    def _safe_add(self, date, state):
        if self.dtype is None:
            self._infer_type(state)

        self.history[date] = state

        # TODO Fix a bug in RedBlackPy which returns the wrong result when the query is in the index (DivisionByZero)
        if self.interpolation == "linear":
            self._index[date] = state

    def iter(self):
        if self.history is not None:
            return rb.SeriesIterator([self.history])()
        else:
            return iter([])
