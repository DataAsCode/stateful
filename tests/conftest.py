# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for stateful.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

# import pytest
import pytest
from stateful import State
import pandas as pd

@pytest.fixture()
def simple_state_empty():
    return State(primary_key="id", time_key="date")

@pytest.fixture()
def conf_state_empty():
    configuration = {
        "kind": {"dtype": "string"},
        "can_make": {"dtype": "string"},
        "amount": {"dtype": "integer", "interpolation": "linear"}
    }
    return State(primary_key="id", time_key="date", configuration=configuration)

@pytest.fixture
def numeric_state():
    df = pd.DataFrame([
        {"id": 0, "date": "2020-12-20", "amount": -100},
        {"id": 0, "date": "2020-12-22", "amount": 50},
        {"id": 0, "date": "2020-12-24", "amount": 100},
        {"id": 1, "date": "2020-12-20", "amount": 100},
        {"id": 1, "date": "2020-12-22", "amount": 50},
        {"id": 1, "date": "2020-12-24", "amount": -100},
        {"id": 2, "date": "2020-12-21", "amount": 100},
        {"id": 2, "date": "2020-12-23", "amount": 50},
        {"id": 2, "date": "2020-12-25", "amount": -100},
    ])
    configuration = {"amount": {"dtype": "integer", "interpolation": "linear"}}

    state = State(primary_key="id", time_key="date", configuration=configuration)
    state.include(df, primary_column="id", columns=["amount"])

    return state

@pytest.fixture
def stateful_state():
    df = pd.DataFrame([
        {"id": 0, "date": "2020-12-20", "amount": -100, "agreement": "cool", "payment": "visa"},
        {"id": 1, "date": "2020-12-22", "amount": 50, "agreement": "cool", "payment": "visa"},
        {"id": 2, "date": "2020-12-24", "amount": 100, "agreement": "not cool", "payment": "mastercard"},
        {"id": 3, "date": "2020-12-20", "amount": 100, "agreement": "uber cool", "payment": "lunar"},
        {"id": 4, "date": "2020-12-22", "amount": 50, "agreement": "cool", "payment": "visa"},
        {"id": 5, "date": "2020-12-24", "amount": -100, "agreement": "not cool", "payment": "mastercard"},
        {"id": 6, "date": "2020-12-21", "amount": 100, "agreement": "cool", "payment": "visa"},
        {"id": 7, "date": "2020-12-23", "weird_stream": "pokemon"},
        {"id": 8, "date": "2020-12-25", "amount": -100, "agreement": "cool", "payment": "visa"},
    ])
    configuration = {"amount": {"dtype": "integer", "interpolation": "linear"}}

    state = State(primary_key="id", time_key="date", configuration=configuration)
    state.include(df, "id", columns=["amount", "agreement", 'payment', "weird_stream"])

    return state