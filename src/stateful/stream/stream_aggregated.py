from stateful.stream.base import BaseStream
import numpy as np


class AggregatedStream(BaseStream):

    def __init__(self, name, configuration: dict, dtype, stream, function):
        BaseStream.__init__(self, name, configuration, dtype, streams=[stream], function=function)
        self.stream = self.streams[0]

    @property
    def start(self):
        return self.stream.start

    @property
    def end(self):
        return self.stream.end

    @property
    def history(self):
        return self.stream.history

    def alias(self, name):
        return AggregatedStream(name, self.configuration, self.dtype, self.stream, self.function)

    def _safe_add(self, date, state):
        raise ValueError("Cannot insert into Aggregated Stream")

    def within(self, date) -> bool:
        return self.stream.within(date)

    def _safe_get(self, date):
        if not self:
            return np.NaN
        return self.function(self.stream._safe_get(date))

    def values(self) -> set:
        return self.stream.values()

    def keys(self) -> set:
        return self.stream.keys()

    def on(self, on=True) -> None:
        self.stream.on(on)

    def iter(self):
        return self.stream.iter()

    def __len__(self):
        return len(self.stream)
