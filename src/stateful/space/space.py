from collections import OrderedDict

import redblackpy as rb
from stateful.representable import Representable
from stateful.stream import Stream
import pandas as pd
from stateful.stream.base import BaseStream
import numpy as np
from stateful.stream.stream_multi import MultiStream
from stateful.utils import list_of_instance


class Space(Representable):
    def __init__(self, primary_key, primary_value, time_key, keys, configuration={}):
        Representable.__init__(self)
        self.time_key = time_key
        self.primary_key = primary_key
        self.primary_value = primary_value
        self._streams = OrderedDict()
        self.keys = keys
        self._configuration = configuration
        self._iter = None
        self._prev = None

    @property
    def start(self):
        return min([stream.start for stream in self._streams.values() if stream.start])

    @property
    def end(self):
        return max([stream.end for stream in self._streams.values() if stream.end])

    @property
    def first(self):
        return self.at_time(self.start)

    @property
    def last(self):
        return self.at_time(self.end)

    def set(self, name, stream):
        assert issubclass(type(stream), BaseStream)
        stream.name = name
        self._streams[name] = stream

    def _strem_conf(self, key):
        if list_of_instance(key, str):
            key = "_".join(key)
        stream_conf = self._configuration.get(key, {})
        stream_conf["time_key"] = self.time_key
        return stream_conf

    def _ensure(self, key):
        if key not in self._streams:
            self._streams[key] = Stream(name=key, configuration=self._strem_conf(key))

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.time_key in event, "Event has to include time key"

        date = event.pop(self.time_key)

        for key, value in event.items():
            self._ensure(key)
            self.keys.add(key)
            self._streams[key].add(date, value)

    def at_time(self, t):
        state = {name: stream.get(t) for name, stream in self._streams.items()}
        state[self.time_key] = t
        state[self.primary_key] = self.primary_value

        for key in self.keys:
            if key not in state:
                state[key] = np.NaN

        return state

    def empty(self):
        return {key: None for key in self.keys}

    def indexes(self):
        idxs = set()
        for stream in self._streams.values():
            idxs = idxs | set(stream.history.index())
        return idxs

    def range(self, start, stop):
        start = pd.to_datetime(start, utc=True)
        stop = pd.to_datetime(start, utc=True)
        return self

    def resample(self, freq):
        return self

    def __iter__(self):
        for stream in self._streams.values():
            stream.on(True)

        self._iter = rb.SeriesIterator([stream.history for stream in self._streams.values()])()
        return self

    def __next__(self):
        try:
            date = next(self._iter)
            state = self.at_time(date)
            return state
        except StopIteration:
            for stream in self._streams.values():
                stream.on(False)

            self._prev = None
            raise StopIteration()

    def __getitem__(self, item):
        try:
            date = pd.to_datetime(item, utc=True)
            return self.at_time(date)
        except:
            if isinstance(item, str):
                self._ensure(item)
                return self._streams[item]
            elif list_of_instance(item, str):
                streams = []
                for name in item:
                    self._ensure(name)
                    streams.append(self._streams[name])
                return MultiStream(configuration=self._strem_conf(item), dtype="multi", streams=streams)
            elif isinstance(item, slice):
                start = item.start if item.start else self.start
                end = item.stop if item.stop else self.end
                space = self.range(start, end)
                if item.step:
                    return space.resample(item.step)
                else:
                    return space

    def __setitem__(self, name, stream):
        stream.name = name
        self._streams[name] = stream

    def head(self, n=5):
        events = list(self)
        if len(events) > (n * 2):
            df = pd.DataFrame(events[:n] + events[-n:])
        else:
            df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        return df

    def df(self):
        events = list(self)
        df = pd.DataFrame(events)
        df = df.set_index(self.time_key)
        return df

    def __len__(self):
        return sum([len(stream) for stream in self._streams.values()])
