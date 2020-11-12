import redblackpy as rb
# from stateful.snapshot import Snapshot
from stateful.stream import Stream
import pandas as pd


class Space:
    def __init__(self, time_key, keys, configuration={}):
        self.time_key = time_key
        self._streams = {}
        self.keys = keys
        self._derivatives = {}
        self._change_derivatives = {}
        self._configuration = configuration
        self._iter = None
        self._prev = None

    @property
    def start(self):
        return min(map(lambda s: s.start, self._streams.values()))

    @property
    def end(self):
        return max(map(lambda s: s.end, self._streams.values()))

    def set(self, name, stream):
        self._streams[name] = stream

    def _ensure(self, key):
        if key not in self._streams:
            self._streams[key] = Stream(self._configuration.get(key, {}))

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.time_key in event, "Event has to include time key"

        date = event.pop(self.time_key)

        for key, value in event.items():
            self._ensure(key)
            self.keys.add(key)
            self._streams[key].add(date, value)

    def add_stream(self, name, stream):
        self._streams[name] = stream

    def _derive(self, data: dict, prev_data: dict):
        data = Snapshot(**data)
        prev_data = Snapshot(**prev_data)

        for name, func in self._derivatives.items():
            data[name] = func(data)

        if prev_data:
            for name, func in self._change_derivatives.items():
                data[name] = func(prev_data, data)

        return data.to_dict()

    def at_time(self, t, stream=None):
        if stream is not None:
            if stream in self._streams:
                return self._streams[stream][t]
            else:
                raise ValueError(f"{stream} not found in state")

        data = {}
        for key, stream in self._streams.items():
            data[key] = stream[t]

        return data

    def add_derivative(self, name, func):
        self._derivatives[name] = func

    def add_change_derivative(self, name, func):
        self._change_derivatives[name] = func

    def empty(self):
        return {key: None for key in self.keys}

    def __getitem__(self, item):
        try:
            date = pd.to_datetime(item, utc=True)
            return self.at_time(date)
        except:
            self._ensure(item)
            if isinstance(item, str):
                return self._streams[item]

    def indexes(self):
        idxs = set()
        for stream in self._streams.values():
            idxs = idxs | set(stream.history.index())
        return idxs

    def __iter__(self):
        for stream in self._streams.values():
            stream.iter(on=True)

        self._iter = rb.SeriesIterator([stream.history for stream in self._streams.values()])()
        return self

    def __next__(self):
        try:
            date = next(self._iter)

            state = {}
            for name, stream in self._streams.items():
                state[name] = stream.get(date)

            if self._prev is None:
                _prev = self.empty()
            else:
                _prev = self._prev

            derived = self._derive(state, _prev)

            self._prev = derived

            derived[self.time_key] = date
            return derived
        except StopIteration:
            for stream in self._streams.values():
                stream.iter(on=False)

            self._prev = None
            raise StopIteration()

    def __len__(self):
        return sum([len(stream) for stream in self._streams.values()])
