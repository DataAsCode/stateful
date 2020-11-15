from stateful.space import Space


class SpaceView(Space):
    def __init__(self, space, view_keys):
        Space.__init__(self, primary_key=space.primary_key, primary_value=space.primary_value, time_key=space.time_key, keys=view_keys)
        self.view_keys = view_keys
        self._streams = space._streams

    def add(self, event: dict):
        raise AttributeError("SpaceView cannot insert new values")

    def at_time(self, t):
        return {name: stream.get(t) for name, stream in self._streams.items() if name in self.view_keys}
