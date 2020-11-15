from stateful.stream.base import BaseStream
import numpy as np
import redblackpy as rb


class MultiStream(BaseStream):

    def __init__(self, configuration: dict, dtype, streams, function=lambda x: x, name=None):
        BaseStream.__init__(self, name, configuration, dtype, streams=streams, function=function)

    @property
    def start(self):
        return min([stream.start for stream in self.streams if stream.start])

    @property
    def end(self):
        return max([stream.end for stream in self.streams if stream.end])

    @property
    def history(self):
        keys = self.keys()
        return rb.Series(index=list(keys), values=list(range(len(keys))))

    def alias(self, name):
        return MultiStream(self.configuration, self.dtype, self.streams, self.function, name=name)

    def values(self) -> set:
        raise ValueError("values cannot be extracted from Multi Stream")

    def keys(self) -> set:
        keys = set()
        for stream in self.streams:
            keys = keys | stream.keys()
        return keys

    def within(self, date) -> bool:
        return self.start <= date <= self.end

    def _safe_add(self, date, state):
        raise ValueError("Cannot insert into Multi Stream")

    def on(self, on=True) -> None:
        for stream in self.streams:
            stream.on(on)

    def __len__(self):
        return len(self.keys())

    def _safe_get(self, date):
        values = {stream.name: stream._safe_get(date) for stream in self.streams}
        values[self.configuration["time_key"]] = date
        return self.function(values)

    def iter(self):
        return rb.SeriesIterator([stream.history for stream in self.streams])()
