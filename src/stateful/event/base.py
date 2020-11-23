from stateful.representable import Representable


class EventBase(Representable):

    def items(self):
        raise NotImplementedError("items() should be implemented by all children")
