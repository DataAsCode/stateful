from itertools import repeat
from random import choice

import pandas as pd
from stateful.representable import Representable
from stateful.space.space import Space
from stateful.utils import list_of_instance


class State(Representable):
    def __init__(self, primary_key, time_key="date", configuration=None, stream_names=None):
        Representable.__init__(self)

        self.primary_key = primary_key
        self.time_key = time_key
        self._all_stream_names = stream_names if stream_names else set()
        self.all_spaces = {}
        self.configuration = configuration if configuration else {}

    @property
    def start(self):
        return min([state.start for state in self.all_spaces.values()])

    @property
    def end(self):
        return max([state.end for state in self.all_spaces.values()])

    @property
    def keys(self):
        return self._all_stream_names

    @property
    def space(self):
        class SpaceGetter:
            def __getitem__(_, item):
                self._ensure(item)
                return self.all_spaces[item]

            def __setitem__(_, name, stream):
                self._ensure(name)
                stream.name = "value"
                self.all_spaces[name]["value"] = stream

        return SpaceGetter()

    def iter_spaces(self):
        return iter(self.all_spaces.items())

    def _ensure(self, key):
        if not key in self.all_spaces:
            self.all_spaces[key] = Space(self.primary_key, key, self.time_key, self._all_stream_names,
                                         self.configuration)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.primary_key in event, "Event has to include primary key"
        assert self.time_key in event, "Event has to include time key"
        key = event.pop(self.primary_key)

        self._ensure(key)
        self.all_spaces[key].add(event)

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

    def rand_choice(self):
        attempts = 10
        space = choice(list(self.all_spaces.values()))
        while not space and attempts > 0:
            attempts -= 1
            space = choice(list(self.all_spaces.values()))

        return space

    def __getitem__(self, item):
        from stateful.state.state_view import StateView

        if isinstance(item, str):
            return StateView(self, item)
        elif list_of_instance(item, str):
            return StateView(self, *item)
        elif isinstance(item, slice):
            # do your handling for a slice object:
            print(item.start, item.stop, item.step)

    def __setitem__(self, name, state):
        if isinstance(name, str) and isinstance(state, State) and len(state.keys) == 1 and "value" in state.keys:
            for primary_key, space in state.spaces:
                self.all_spaces[primary_key][name] = space["value"]

    def __len__(self):
        return len(self.dates())

    def dates(self):
        all_dates = set()
        for space in self.all_spaces.values():
            all_dates = all_dates | space.indexes()
        return all_dates

    def head(self, n=5):
        events = []
        for _ in range(n):
            space = self.rand_choice()
            events += [space.first, space.last]

        df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        df = df.sort_index()
        return df

    def df(self):
        events = []
        for space_events in map(list, self.all_spaces.values()):
            events += space_events

        df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        df = df.sort_index()
        return df

    def infer(self, freq="1d"):
        pass
