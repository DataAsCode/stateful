import pandas as pd
import numpy as np


def is_type(v, t):
    return isinstance(v, t) or issubclass(type(v), t)


def list_of_instance(arr, type):
    if isinstance(arr, (list, tuple, set)):
        return all([is_type(v, type) for v in arr])
    return False


def dict_of_instance(data, key_type, value_type):
    if isinstance(data, dict):
        return list_of_instance(list(data.keys()), key_type) and list_of_instance(list(data.values()), value_type)
    return False


def cast_output_numpy(dtype, arr: np.ndarray):
    if dtype == "integer":
        return arr.astype(int)
    elif dtype == "boolean":
        return arr.astype(bool)
    else:
        return arr


def cast_output(dtype, value):
    from stateful.event.event import Event
    if isinstance(value, Event) and value.isna():
        return value.isna()
    elif not isinstance(value, Event) and pd.isna(value):
        return value
    elif isinstance(value, np.ndarray):
        return cast_output_numpy(dtype, value)
    elif dtype == "integer":
        return int(value)
    elif dtype == "boolean":
        return bool(value)
    else:
        return value
