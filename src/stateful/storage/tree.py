from datetime import timedelta

import redblackpy as rb
import pandas as pd
from pandas import DatetimeIndex
from pandas.api.types import infer_dtype
import numpy as np
from stateful.event.event_column import EventColumn


class DateTree:

    def __init__(self,
                 name,
                 dtype,
                 index=None,
                 values=None,
                 interpolation="floor",
                 on_dublicate="increment",
                 tree=None,
                 change_tree=None,
                 backup_index=None):
        self.name = name
        self.dtype = dtype
        self.on_dublicate = on_dublicate
        self.interpolation = interpolation

        if not backup_index:
            if index:
                self._backup_index = {idx: value for idx, value in zip(index, values)}
            else:
                self._backup_index = {}
        else:
            self._backup_index = backup_index

        self._iter = iter([])

        if tree is None:
            self._tree = rb.Series(dtype=self._infer_dtype(self.dtype),
                                   index=index,
                                   values=values,
                                   interpolate=interpolation)
        else:
            self._tree = tree

        if change_tree is None:
            if index is not None and values is not None:
                change_idx, change_vals = [], []
                for idx, value in zip(index, values):
                    if not change_vals or change_vals[-1] != value:
                        change_idx.append(idx)
                        change_vals.append(value)
            else:
                change_idx, change_vals = None, None

            self._change_tree = rb.Series(dtype=self._infer_dtype(self.dtype),
                                          index=change_idx,
                                          values=change_vals,
                                          interpolate="floor")
        else:
            self._change_tree = change_tree

        if index is not None:
            self.length = len(index)
        elif tree is not None:
            self.length = len(tree)
        else:
            self.length = 0

    @property
    def empty(self):
        return self.length == 0

    @property
    def default(self):
        if self.dtype in {"integer", "floating"}:
            return 0
        elif self.dtype == "boolean":
            return False
        else:
            return np.NaN

    @property
    def start(self):
        return self._tree.begin() if not self.empty else None

    @property
    def end(self):
        return self._tree.end() if not self.empty else None

    @property
    def first(self):
        if self.empty:
            return self.default

        return self.get(self.start)

    @property
    def last(self):
        if self.empty:
            return self.default

        return self.get(self.end)

    @staticmethod
    def from_example(name, configuration, date, example):
        if "dtype" not in configuration:
            configuration["dtype"] = infer_dtype([example])
        tree = DateTree(name, index=[pd.to_datetime(date, utc=True)], values=[example], **configuration)
        return tree

    def iterable(self):
        return [self._tree] if not self.empty else []

    def alias(self, name):
        return DateTree(name,
                        dtype=self.dtype,
                        interpolation=self.interpolation,
                        on_dublicate=self.on_dublicate,
                        tree=self._tree,
                        change_tree=self._change_tree,
                        backup_index=self._backup_index)

    def values(self):
        return self._tree.values()

    def dates(self):
        return list(self._tree.index())

    def _safe_get(self, date):
        if date in self._backup_index:
            return self._backup_index[date]
        else:
            return self._tree[date]

    def get(self, date):
        if self.empty:
            return self.default

        date = pd.to_datetime(date, utc=True)

        if self.start > date:
            result = self.default
        elif self.end < date:
            if self.interpolation != "floor":
                result = self.default
            else:
                result = self.last
        else:
            result = self._safe_get(date)

        return result

    def all(self, dates: DatetimeIndex):
        start, end = self.start, self.end

        before, during, after = start > dates, (start <= dates) & (end >= dates), end < dates

        if self.dtype == "string":
            dtype = f"object"
        else:
            dtype = self._infer_dtype(self.dtype)

        values = np.empty(len(dates), dtype=dtype)
        if self.interpolation == "linear":
            values[np.argwhere(before)] = self.default
            values.flat[np.argwhere(during)] = [self._safe_get(date) for date in dates[during]]
            values[np.argwhere(after)] = self.default
        else:
            values[np.argwhere(before)] = self.default
            values.flat[np.argwhere(during)] = [self._change_tree[date] for date in dates[during]]
            values[np.argwhere(after)] = self.last

        return EventColumn(name=self.name, dates=dates, events=values)

    def _safe_add(self, date, value):
        previous_value = self.floor(date)
        if value != previous_value:
            self._change_tree[date] = value

        self._tree[date] = value

        if self.interpolation == "linear":
            self._backup_index[date] = value

        self.length += 1

    def add(self, date, value):
        date = pd.to_datetime(date, utc=True)
        value = self.cast_input(value)
        try:
            self._safe_add(date, value)
        except KeyError:
            if self.on_dublicate == "increment":
                date = date + timedelta(seconds=1)
                self.add(date, value)

    def cast_input(self, value):
        if pd.isna(value):
            return value

        if self.dtype == "integer":
            return int(value)
        elif self.dtype == "boolean":
            return int(value)
        else:
            return value

    def within(self, date) -> bool:
        if self.empty:
            return False

        return self.start <= date <= self.end

    def floor(self, date):
        try:
            return self._tree.floor(date)
        except:
            return np.NaN, self.default

    def ceil(self, date):
        try:
            return self._tree.ceil(date)
        except:
            return np.NaN, self.default

    def on(self, on=True):
        if on:
            self._tree.on_itermode()
        else:
            self._tree.off_itermode()

    @staticmethod
    def _infer_dtype(dtype):
        if dtype == "string":
            rb_dtype = "str"
        elif dtype == "integer":
            rb_dtype = "float32"
        elif dtype == "floating":
            rb_dtype = "float32"
        elif dtype == "boolean":
            rb_dtype = "uint8"
        else:
            rb_dtype = "object"

        return rb_dtype

    def __len__(self):
        return self.length

    def __iter__(self):
        if not self.empty:
            self.on(True)
            self._iter = rb.SeriesIterator([self._tree])()
        else:
            self._iter = iter([])

        return self

    def __next__(self):
        try:
            date = next(self._iter)
            return date, self.get(date)
        except StopIteration:
            if not self.empty:
                self.on(False)
            self._iter = iter([])
