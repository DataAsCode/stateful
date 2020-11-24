from datetime import datetime
import numpy as np
from pandas import DatetimeIndex
from stateful.utils import infer_dtype, list_of_instance


class EventColumn:

    def __init__(self, name, dates: DatetimeIndex, events):
        assert len(dates) == len(events)
        self.name = name
        self.dates = dates
        self.events = events if isinstance(events, np.ndarray) else events.events
        try:
            self.dtype = infer_dtype(self.events[~np.isnan(self.events)][0])
        except:
            self.dtype = None

    def apply(self, function):
        try:
            new_events = function(self.events)
        except:
            new_events = np.array([function(event) for event in self.events])

        return EventColumn(self.name, self.dates, new_events)

    def cast(self, dtype=None):
        dtype = dtype if dtype else self.dtype

        events = self.events
        if dtype == "integer":
            events = events.astype(int)
        elif dtype == "boolean":
            events = events.astype(bool)
        else:
            events = events

        return EventColumn(self.name, self.dates, events)

    def __iter__(self):
        for date, event in zip(self.dates, self.events):
            yield date, event

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.events[item]

    def __contains__(self, item):
        if isinstance(item, datetime):
            return item in self.dates
        elif list_of_instance(item, datetime):
            return all([item in self.dates])
        else:
            return item in self.events

    def __len__(self):
        return len(self.dates)

    def __add__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events + other.events)
        else:
            return EventColumn(self.name, self.dates, self.events + other)

    def __radd__(self, other):
        if isinstance(other, EventColumn):
            return other.events + self.events
        else:
            return other + self.events

    def __sub__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events - other.events)
        else:
            return EventColumn(self.name, self.dates, self.events - other)

    def __rsub__(self, other):
        if isinstance(other, EventColumn):
            return other.events - self.events
        else:
            return other - self.events

    def __mul__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events * other.events)
        else:
            return EventColumn(self.name, self.dates, self.events * other)

    def __rmul__(self, other):
        if isinstance(other, EventColumn):
            return other.events * self.events
        else:
            return other * self.events

    def __pow__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events ** other.events)
        else:
            return EventColumn(self.name, self.dates, self.events ** other)

    def __rpow__(self, other):
        if isinstance(other, EventColumn):
            return other.events ** self.events
        else:
            return other ** self.events

    def __mod__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events % other.events)
        else:
            return EventColumn(self.name, self.dates, self.events % other)

    def __rmod__(self, other):
        if isinstance(other, EventColumn):
            return other.events % self.events
        else:
            return other % self.events

    def __floordiv__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events // other.events)
        else:
            return EventColumn(self.name, self.dates, self.events // other)

    def __rfloordiv__(self, other):
        if isinstance(other, EventColumn):
            return other.events // self.events
        else:
            return other // self.events

    def __truediv__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events / other.events)
        else:
            return EventColumn(self.name, self.dates, self.events / other)

    def __rtruediv__(self, other):
        if isinstance(other, EventColumn):
            return other.events / self.events
        else:
            return other / self.events

    def __and__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events & other.events)
        else:
            return EventColumn(self.name, self.dates, self.events & other)

    def __rand__(self, other):
        if isinstance(other, EventColumn):
            return other.events & self.events
        else:
            return other & self.events

    def __or__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events | other.events)
        else:
            return EventColumn(self.name, self.dates, self.events | other)

    def __ror__(self, other):
        if isinstance(other, EventColumn):
            return other.events | self.events
        else:
            return other | self.events

    def __eq__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events == other.events)
        else:
            return EventColumn(self.name, self.dates, self.events == other)

    def __neq__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events != other.events)
        else:
            return EventColumn(self.name, self.dates, self.events != other)

    def __gt__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events > other.events)
        else:
            return EventColumn(self.name, self.dates, self.events > other)

    def __ge__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events >= other.events)
        else:
            return EventColumn(self.name, self.dates, self.events >= other)

    def __lt__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events < other.events)
        else:
            return EventColumn(self.name, self.dates, self.events < other)

    def __le__(self, other):
        if isinstance(other, EventColumn):
            return EventColumn(f"{self.name}_{other.name}", self.dates, self.events <= other.events)
        else:
            return EventColumn(self.name, self.dates, self.events <= other)

    def __neg__(self):
        return EventColumn(self.name, self.dates, -self.events)

    def __pos__(self):
        return EventColumn(self.name, self.dates, self.events.__pos__())

    def __abs__(self):
        return EventColumn(self.name, self.dates, self.events.__abs__())

    def __invert__(self):
        return EventColumn(self.name, self.dates, self.events.__invert__())

    def __str__(self):
        return f"EventColumn({self.name}, {self.dates}, {self.events})"

    def __repr__(self):
        return str(self)
