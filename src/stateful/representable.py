import pandas as pd


class Representable:

    @property
    def empty(self):
        raise NotImplementedError("empty() should be implemented by all children")

    def df(self) -> pd.DataFrame:
        raise NotImplementedError("df() should be implemented by all children")

    def head(self, n=5) -> pd.DataFrame:
        raise NotImplementedError("head() should be implemented by all children")

    def __repr__(self):
        if not self.empty:
            return self.head().__repr__()
        return str(self)

    def _repr_data_resource_(self):
        if not self.empty:
            return self.head()._repr_data_resource_()
        return str(self)

    def _repr_fits_horizontal_(self):
        if not self.empty:
            return self.head()._repr_fits_horizontal_()
        return str(self)

    def _repr_fits_vertical_(self):
        if not self.empty:
            return self.head()._repr_fits_vertical_()
        return str(self)

    def _repr_html_(self):
        if not self.empty:
            return self.head()._repr_html_()
        return str(self)

    def __str__(self):
        return f"{type(self).__name__}"
