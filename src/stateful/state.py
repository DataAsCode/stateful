from datetime import datetime
from itertools import repeat
from random import choice
from typing import Dict

import pandas as pd
from pandas import DatetimeIndex
from stateful.state_transposed import TransposedState
from stateful.representable import Representable
from stateful.space import Space
from stateful.storage.calculated_stream import CalculatedStream
from stateful.storage.stream_graph import StreamGraph
from stateful.utils import list_of_instance


class State(Representable):
    def __init__(self, primary_key, time_key="date", configuration=None, stream_name=None):
        Representable.__init__(self)
        self.primary_key = primary_key
        self.time_key = time_key

        self.all_spaces = {}
        self.configuration = configuration if configuration else {}
        self.graph = StreamGraph(stream_name if stream_name else {key for key in self.configuration.keys()})

    @property
    def start(self):
        return min([state.start for state in self.all_spaces.values()])

    @property
    def end(self):
        return max([state.end for state in self.all_spaces.values()])

    @property
    def keys(self):
        return self.graph.keys

    @property
    def empty(self):
        return bool(self.all_spaces) and len(self) == 0

    @property
    def space(self):
        class SpaceGetter:
            def __getitem__(_, item):
                self._ensure(item)
                return self.all_spaces[item]

            def random(_):
                non_empty_spaces = [space for space in self.all_spaces.values() if not space.empty]
                if non_empty_spaces:
                    return choice(non_empty_spaces)
                else:
                    raise ValueError("Only empty spaces avialable")

        return SpaceGetter()

    def transpose(self, freq="1d"):
        return TransposedState(self, freq)

    def cross(self, freq="1d"):
        return self.transpose(freq)

    def _ensure(self, key):
        if not key in self.all_spaces:
            self.all_spaces[key] = Space(primary_key=self.primary_key,
                                         primary_value=key,
                                         time_key=self.time_key,
                                         graph=self.graph,
                                         configuration=self.configuration)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.primary_key in event, "Event has to include primary key"
        assert self.time_key in event, "Event has to include time key"
        key = event.pop(self.primary_key)

        for name in event.keys():
            if name != self.time_key:
                self.graph.add(name, [])

        self._ensure(key)
        self.all_spaces[key].add(event)

    def set(self, name, space):
        self.all_spaces[name] = space

    def include(self, df, primary_column=None, time_column=None, event=None, columns=None, drop_na=False, fill_na=None):
        assert event is not None or columns is not None
        assert event is None or isinstance(event, dict)
        assert columns is None or isinstance(columns, (dict, list))
        assert primary_column or self.primary_key in df.columns
        assert time_column or self.time_key in df.columns

        primary_column = primary_column if primary_column else self.primary_key
        time_column = time_column if time_column else self.time_key

        if not isinstance(df, pd.DataFrame):
            df = df.df()

        if columns is None:
            columns = {}
        elif isinstance(columns, list):
            columns = {column: column for column in columns}

        if columns:
            extra_vals = df.rename(columns=columns)[list(columns.values())].to_dict(orient="records")
        else:
            extra_vals = repeat({})

        for (id, time), extra in zip(df[[primary_column, time_column]].values, extra_vals):
            if pd.isna(time):
                if drop_na:
                    continue
                else:
                    time = fill_na

            row = {self.primary_key: id, self.time_key: time}

            row.update(extra)
            if event:
                row.update(event)

            self.add(row)

    def all(self, dates):
        for name, space in self.all_spaces.items():
            events = space.all(dates)
            events.add_column(self.primary_key, name)
            yield events

    def __getitem__(self, item):
        if isinstance(item, str):
            return CalculatedStream([item])
        elif list_of_instance(item, str):
            return CalculatedStream(item)
        elif list_of_instance(item, datetime) or isinstance(item, DatetimeIndex):
            return self.all(dates=item)
        elif isinstance(item, slice):
            # do your handling for a slice object:
            print(item.start, item.stop, item.step)

    def filter(self, function):
        state = State(self.primary_key, self.time_key, self.configuration)
        for name, space in self.all_spaces.items():
            if function(space):
                state.set(name, space)
        return state

    def __setitem__(self, name, item):
        assert isinstance(item, CalculatedStream)
        self.graph.add(name, item.dependencies)

        for space in self.all_spaces.values():
            space[name] = item.assign_to(space)

    def __len__(self):
        return sum([len(space) for space in self.all_spaces.values()])

    def dates(self):
        all_dates = set()
        for space in self.all_spaces.values():
            all_dates = all_dates | space.indexes()
        return all_dates

    def head(self, n=5):
        events = []
        for _ in range(n):
            space = self.space.random()
            events += [space.first.value, space.last.value]

        df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        df = df.sort_index()
        return df

    def df(self):
        events = []
        for space_events in map(list, self.all_spaces.values()):
            events += [event.value for event in space_events]

        df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        df = df.sort_index()
        return df

    def infer(self, freq="1d"):
        pass
