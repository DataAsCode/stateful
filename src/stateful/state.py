from collections import defaultdict
from itertools import repeat
from random import choice

import pandas as pd
import redblackpy as rb
from stateful.space import Space


class State:
    def __init__(self, primary_key, time_key, configuration={}):
        self.primary_key = primary_key
        self.time_key = time_key
        self._all_stream_names = set()
        self._state_spaces = defaultdict(lambda: Space(time_key, self._all_stream_names, configuration))
        self.configuration = configuration

    @property
    def start(self):
        return min([state.start for state in self._state_spaces.values()])

    @property
    def end(self):
        return max([state.end for state in self._state_spaces.values()])

    def set(self, name, series):
        for space in self._state_spaces.values():
            space.set(name, series)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.primary_key in event, "Event has to include primary key"
        assert self.time_key in event, "Event has to include time key"

        key = event.pop(self.primary_key)
        self._state_spaces[key].add(event)

    def include(self, df, primary_key, time_column, event: dict = None, columns=None, drop_na=False, fill_na=None):
        assert event is not None or columns is not None
        assert event is None or isinstance(event, dict)
        assert columns is None or isinstance(columns, (dict, list))

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

        for (id, time), extra in zip(df[[primary_key, time_column]].values, extra_vals):
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

    def derive(self, name, function, include_previous=False):
        for space in self._state_spaces.values():
            if include_previous:
                space.add_change_derivative(name, function)
            else:
                space.add_derivative(name, function)

        return self

    def get_random(self):
        return choice(list(self._state_spaces.values()))

    def __getitem__(self, item):
        return self._state_spaces[item]

    def __len__(self):
        return len(self.dates())

    def df(self, limit=-1):
        all_events = []

        for key, state in self._state_spaces.items():
            for i, event in enumerate(state):
                event[self.primary_key] = key
                all_events.append(event)
                if 0 < limit < i:
                    break

        df = pd.DataFrame(all_events)
        df = df.set_index(self.time_key).sort_index()
        return df

    def dates(self):
        all_dates = set()
        for space in self._state_spaces.values():
            all_dates = all_dates | space.indexes()
        return all_dates

    def head(self, n_pr_key=1):
        return self.df(n_pr_key)

    def infer(self, freq="1d"):
        pass
