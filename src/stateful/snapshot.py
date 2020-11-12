
class Snapshot:

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __getitem__(self, item):
        return self._kwargs.get(item)

    def __setitem__(self, key, value):
        self._kwargs[key] = value

    def to_dict(self):
        return self._kwargs