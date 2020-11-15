import pandas as pd


class Representable:

    def df(self) -> pd.DataFrame:
        raise NotImplementedError("df() should be implemented by all children")

    def head(self, n=5) -> pd.DataFrame:
        raise NotImplementedError("head() should be implemented by all children")

    def __repr__(self):
        return self.head().__repr__()

    def _repr_data_resource_(self):
        return self.head()._repr_data_resource_()

    def _repr_fits_horizontal_(self):
        return self.head()._repr_fits_horizontal_()

    def _repr_fits_vertical_(self):
        return self.head()._repr_fits_vertical_()

    def _repr_html_(self):
        return self.head()._repr_html_()
