from stateful.space.space_view import SpaceView
from stateful.state.state import State


class StateView(State):
    def __init__(self, space, *view_keys):
        self.view_keys = set(view_keys)
        State.__init__(self, space.primary_key, space.time_key, stream_names=self.view_keys)
        self.all_spaces = {name: SpaceView(space, self.view_keys) for name, space in space.iter_spaces()}

    def add(self, event: dict):
        raise AttributeError("StateView cannot insert new values")

    def include(self, df, primary_column=None, time_column=None, event=None, columns=None, drop_na=False, fill_na=None):
        raise AttributeError("StateView cannot insert new values")

    def apply(self, function):
        state = State(self.primary_key, self.time_key)

        for name, space in self.iter_spaces():
            state.space[name] = space[list(self.view_keys)].apply(function)

        return state
