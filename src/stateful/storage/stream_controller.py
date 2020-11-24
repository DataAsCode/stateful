from typing import List, Optional

from pandas import DatetimeIndex
from stateful.event.event import Event
from stateful.event.event_frame import EventFrame
from stateful.storage.stream import Stream
from stateful.utils import list_of_instance, cast_output
from pandas.api.types import infer_dtype

import pandas as pd
import numpy as np
import redblackpy as rb


class StreamController:

    def __init__(self, graph, configuration):
        from stateful.storage.stream_graph import StreamGraph
        self.graph: StreamGraph = graph
        self.configuration = configuration

        self.streams = {}

    @property
    def data_streams(self):
        return [stream for stream in self.streams.values() if isinstance(stream, Stream)]

    @property
    def start(self):
        return min([stream.start for stream in self.data_streams if stream.start])

    @property
    def end(self):
        return max([stream.end for stream in self.data_streams if stream.end])

    @property
    def empty(self):
        return not (self.data_streams and any([not stream.empty for stream in self.data_streams]))

    @property
    def keys(self):
        return self.graph.keys

    def _stream_conf(self, key):
        if list_of_instance(key, str):
            key = "_".join(key)
        stream_conf = self.configuration.get(key, {})
        stream_conf['on_dublicate'] = 'increment'
        return stream_conf

    def add_stream(self, name, stream, dependencies: Optional[List[str]] = None):
        from stateful.storage.calculated_stream import CalculatedStream

        if isinstance(stream, CalculatedStream):
            dependencies = stream.dependencies if not dependencies else dependencies + stream.dependencies

        self.graph.add(name, dependencies)
        self.streams[name] = stream

    def ensure_stream(self, key, value=None):
        self.graph.add(key, [])

        if key not in self.streams:
            configuration = self._stream_conf(key)
            dtype = configuration.get("dtype", infer_dtype([value]) if value else None)
            self.add_stream(key, Stream(name=key, dtype=dtype, configuration=configuration))

    def get(self, date, columns=None, cast=True):
        from stateful.storage.calculated_stream import CalculatedStream

        state = Event(date, state={})
        for name, dependencies in self.graph.execution_order(columns):
            if name in self.streams:
                if dependencies:
                    assert isinstance(self.streams[name], CalculatedStream)

                    state[name] = self.streams[name].calculate(state[dependencies], name)
                else:
                    state[name] = self.streams[name].get(date, cast=False)
            else:
                state[name] = np.NaN

        if not columns:
            for name in self.keys:
                if name not in state.keys():
                    state[name] = np.NaN

        if cast:
            for name, stream in state.items():
                if name in self.streams:
                    state[name] = cast_output(self.streams[name].dtype, state[name])

        return state

    def all(self, dates=None, columns=None, cast=True):
        from stateful.storage.calculated_stream import CalculatedStream

        if not dates:
            dates = DatetimeIndex(list(self))

        state = EventFrame(dates)
        for name, dependencies in self.graph.execution_order(columns):
            if name not in self.streams:
                state.add_column(state.empty_col(name, size=len(state)))
            else:
                stream = self.streams[name]
                if dependencies:
                    assert isinstance(stream, CalculatedStream)
                    state.add_column(stream.calculate(state[dependencies], name))
                else:
                    state.add_column(stream.all(dates, cast=False))

        for name in self.keys:
            if name not in state:
                state.add_column(state.empty_col(name, size=len(state)))

        if cast:
            for name, column in state.items():
                if name in self.streams:
                    state[name] = column.cast(self.streams[name].dtype)

        return state

    def __contains__(self, item):
        return item in self.streams

    def __getitem__(self, item):
        assert isinstance(item, str)
        self.ensure_stream(item, [])
        return self.streams[item]

    def on(self, on=True):
        for stream in self.data_streams:
            stream.on(on)

    def __iter__(self):
        if self.empty:
            return iter([])
        else:
            self.on(True)
            iterables = []
            for stream in self.data_streams:
                iterables += stream.tree.iterable()

            return rb.SeriesIterator(iterables)() if iterables else iter([])
