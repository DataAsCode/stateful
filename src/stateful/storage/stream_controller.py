from typing import List, Optional
from stateful.event.event import Event
from stateful.event.event_frame import EventFrame
from stateful.storage.stream import Stream
from stateful.utils import list_of_instance, cast_output
from pandas.api.types import infer_dtype
import networkx as nx
import pandas as pd
import numpy as np
import redblackpy as rb


class StreamController:
    __root__ = "<ROOT>"

    def __init__(self, get_keys, configuration):
        self._get_keys = get_keys
        self.configuration = configuration

        self.streams = {}
        self.DAG = nx.DiGraph()
        self.DAG.add_node(self.__root__)

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
        return self.data_streams and any([not stream.empty for stream in self.data_streams])

    @property
    def keys(self):
        return self._get_keys()

    def _stream_conf(self, key):
        if list_of_instance(key, str):
            key = "_".join(key)
        stream_conf = self.configuration.get(key, {})
        stream_conf['on_dublicate'] = 'increment'
        return stream_conf

    def add_stream(self, name, stream, dependencies: Optional[List[str]] = None):
        self.DAG.add_node(name)
        if not dependencies:
            self.DAG.add_edge(self.__root__, name)
        else:
            for dependency in dependencies:
                self.DAG.add_edge(dependency, name)

        self.streams[name] = stream

        all_keys = self._get_keys()
        if name not in all_keys:
            all_keys.add(name)


    def ensure_stream(self, key, value=None):
        all_keys = self._get_keys()
        if key not in all_keys:
            all_keys.add(key)

        if key not in self.streams:
            configuration = self._stream_conf(key)
            dtype = configuration.get("dtype", infer_dtype([value]) if value else None)
            self.add_stream(key, Stream(name=key, dtype=dtype, configuration=configuration))

    def _target_columns(self, columns):
        required_columns = set()

        target_columns = set(columns)
        while target_columns:
            column = target_columns.pop()

            required_columns.add(column)

            target_columns = (target_columns | set(self.DAG.predecessors(column))).difference(self.__root__)

        return required_columns

    def ordering(self, columns):
        required_columns = set(self.DAG.nodes) if not columns else self._target_columns(columns)

        state = set()
        for name in self.DAG.successors(self.__root__):
            if name in required_columns and name != self.__root__:
                state.add(name)
                yield name, []

        missing_streams = [name for name in required_columns if name not in state and name != self.__root__]
        while missing_streams:
            for name in missing_streams:
                dependencies = self.DAG.predecessors(name)
                if all([dependency in state for dependency in dependencies if dependencies != self.__root__]):
                    state.add(name)
                    yield name, dependencies

            missing_streams = [node for node in self.DAG.nodes if node not in state != self.__root__]

    def get(self, date, columns=None, cast=True):
        from stateful.storage.calculated_stream import CalculatedStream

        state = Event(date, state={})
        for name, dependencies in self.ordering(columns):
            if dependencies:
                assert isinstance(self.streams[name], CalculatedStream)
                state[name] = self.streams[name].get(state[dependencies])
            else:
                state[name] = self.streams[name].get(date, cast=False)

        if not columns:
            for name in self.keys:
                if name not in state.keys():
                    state[name] = np.NaN

        if cast:
            for name, stream in state.items():
                state[name] = cast_output(self.streams[name].dtype, state[name])

        return state

    def all(self, dates, columns=None, cast=True):
        from stateful.storage.calculated_stream import CalculatedStream

        state = EventFrame(dates)
        for name, dependencies in self.ordering(columns):
            if dependencies:
                assert isinstance(self.streams[name], CalculatedStream)
                state.add_column(name, self.streams[name].all(state[dependencies]))
            else:
                state.add_column(name, self.streams[name].all(dates, cast=False))

        for name in self.keys:
            if name not in state:
                state.add_column(name, state.empty_col(len(state)))

        if cast:
            for name, stream in state.items():
                state[name] = cast_output(self.streams[name].dtype, state[name])

        return state

    def __contains__(self, item):
        return item in self.streams

    def __getitem__(self, item):
        assert isinstance(item, str)
        self.ensure_stream(item)
        return self.streams[item]

    def on(self, on=True):
        for stream in self.data_streams:
            stream.on(on)

    def __iter__(self):
        if self.empty:
            return iter([])
        else:
            self.on()
            iterables = []
            for stream in self.data_streams:
                iterables += stream.tree.iterable()

            return rb.SeriesIterator(iterables)() if iterables else iter([])
