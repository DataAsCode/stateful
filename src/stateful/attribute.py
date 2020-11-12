from collections import defaultdict


class Attribute:
    def __init__(self, spaces: defaultdict):
        self._spaces = spaces

    def __setitem__(self, name, stream):
        pass

    def __getitem__(self, item):
        pass
