from datetime import datetime

from stateful.storage.stream_controller import StreamController
from stateful.representable import Representable
import pandas as pd
from stateful.utils import list_of_instance


class Space(Representable):

    def __init__(self, primary_key, primary_value, time_key, get_keys, configuration=None):
        Representable.__init__(self)
        self.time_key = time_key
        self.primary_key = primary_key
        self.primary_value = primary_value
        self.controller = StreamController(get_keys, configuration)
        self._iter = None
        self._prev = None
        self.length = 0

    @property
    def start(self):
        return self.controller.start

    @property
    def end(self):
        return self.controller.end

    @property
    def first(self):
        return self.controller.get(self.start)

    @property
    def last(self):
        return self.controller.get(self.end)

    @property
    def empty(self):
        return len(self) == 0

    @property
    def keys(self):
        return self.controller.keys

    def set(self, name, stream):
        self.controller.add_stream(name, stream)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.time_key in event, "Event has to include time key"
        self.length += 1

        date = event.pop(self.time_key)

        for key, value in event.items():
            self.controller[key].add(date, value)

    def get(self, date, include_date=True, include_id=True):
        event = self.controller.get(date)

        if include_date:
            event[self.time_key] = date
        if include_id:
            event[self.primary_key] = self.primary_value

        return event

    def all(self, times):
        return self.controller.all(times)

    def add_stream(self, name):
        self.controller.ensure_stream(name)

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

    def __iter__(self):
        self._iter = iter(self.controller)
        return self

    def __next__(self):
        try:
            date = next(self._iter)
            state = self.get(date)
            return state
        except StopIteration:
            self.controller.on(False)
            raise StopIteration()

    def __getitem__(self, item):
        from stateful.storage.calculated_stream import CalculatedStream

        if (isinstance(item, str) and ("/" in item or "-" in item)) or isinstance(item, datetime):
            date = pd.to_datetime(item, utc=True)
            return self.get(date, include_date=False, include_id=False)
        if list_of_instance(item, datetime):
            return list(self.all(item))
        elif isinstance(item, str):
            assert item in self.controller.keys, f"{item} not valid a stream"
            return CalculatedStream(dependencies=[item], parent=self)
        elif list_of_instance(item, str):
            for name in item:
                assert name in self.controller.keys, f"{name} not valid a stream"

            return CalculatedStream(dependencies=item, parent=self)

    def __setitem__(self, name, stream):
        self.controller.add_stream(name, stream)

    def __bool__(self):
        return len(self) > 0

    def __len__(self):
        return self.length
