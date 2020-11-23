from typing import Optional

import numpy as np
import pandas as pd
from stateful.representable import Representable
from stateful.storage.tree import DateTree
from stateful.utils import list_of_instance
from pandas.api.types import infer_dtype


class Stream(Representable):

    def __init__(self, name, configuration: dict = None, dtype=None, tree: Optional[DateTree] = None):
        Representable.__init__(self)
        self.name = name
        self.dtype = dtype if dtype else configuration.get("dtype")
        self._tree = tree
        self.configuration = configuration if configuration else {}

    @property
    def length(self):
        return len(self._tree) if self._tree else None

    @property
    def tree(self):
        if self._tree is None:
            self._tree = DateTree(self.name, self.dtype)
        return self._tree

    @property
    def interpolation(self):
        return self.tree.interpolation

    @property
    def empty(self):
        return not (self.tree is not None and not self.tree.empty)

    @property
    def start(self):
        return self.tree.start

    @property
    def end(self):
        return self.tree.end

    @property
    def first(self):
        return self.tree.first

    @property
    def last(self):
        return self.tree.last

    def set_name(self, name):
        self.name = name
        self._tree.name = name

    def ceil(self, date):
        return self.tree.ceil(date)

    def floor(self, date):
        return self.tree.floor(date)

    def head(self, n=5) -> pd.DataFrame:
        if self.empty:
            return pd.DataFrame()

        index, values = [], []
        iterator = iter(self._tree)

        while len(index) < n:
            idx, value = next(iterator)
            index.append(idx)
            values.append(value)

        if list_of_instance(values, dict):
            return pd.DataFrame(values, index=index)
        else:
            name = self.name if self.name else "values"
            columns = [{name: v} for v in values]
            return pd.DataFrame(columns, index=index)

    def df(self) -> pd.DataFrame:
        if self.empty:
            return pd.DataFrame()

        index, values = zip(*list(self._tree))

        if list_of_instance(values, dict):
            return pd.DataFrame(values, index=index)
        else:
            return pd.DataFrame(columns={self.name: values}, index=index)

    def alias(self, name):
        return Stream(name, self.configuration, self.dtype, self._tree.alias(name), self.function, self.frozen)

    def values(self):
        return self.tree.values()

    def dates(self):
        return self.tree.dates()

    def on(self, on=True) -> None:
        self._tree.on(on)

    def within(self, date) -> bool:
        return self.tree.within(date)

    def get(self, date, cast=True):
        return self.tree.get(date, cast)

    def all(self, dates, cast=True):
        return self.tree.all(dates, cast)

    def add(self, date, state):
        if pd.isna(state) and self.empty:
            return

        if self.dtype is None:
            self.dtype = infer_dtype([state])

        if self._tree is None or self._tree.empty:
            self._tree = DateTree.from_example(self.name, self.configuration, date, example=state)
        else:
            self._tree.add(date, state)

    """
    List methods
    """

    def __len__(self):
        return self.length

    def __contains__(self, item):
        if self.empty:
            return False

        try:
            date = pd.to_datetime(item, utc=True)
            return self.start <= date <= self.end
        except:
            return item in self.values()

    def __iter__(self):
        return iter(self.tree)

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