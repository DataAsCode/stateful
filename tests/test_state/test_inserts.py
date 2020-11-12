from datetime import datetime
import pandas as pd


def test_individual_inserts_simple(simple_state_empty):
    state = simple_state_empty
    state[1].add({"date": "2020-12-10", "kind": "elf"})
    state[1].add({"date": "2020-12-22", "can_make": "presents"})
    state[1].add({"date": "2020-12-22", "amount": 5})
    state[1].add({"date": "2020-12-24", "amount": 100})

    assert state[1].start == pd.to_datetime("2020-12-10", utc=True)
    assert state[1].end == pd.to_datetime("2020-12-24", utc=True)

    assert state[1]["2020-12-9"] == {"can_make": None, "kind": None, "amount": None}
    assert state[1]["2020-12-10"] == {"can_make": None, "kind": "elf", "amount": None}
    assert state[1][datetime(2020, 12, 22)] == {"can_make": "presents", "kind": "elf", "amount": 5}
    assert state[1][datetime(2020, 12, 23)] == {"can_make": "presents", "kind": "elf", "amount": 5}


def test_individual_inserts_configured(conf_state_empty):
    state = conf_state_empty
    state[1].add({"date": "2020-12-10", "kind": "elf"})
    state[1].add({"date": "2020-12-22", "can_make": "presents"})
    state[1].add({"date": "2020-12-21", "amount": 4})
    state[1].add({"date": "2020-12-22", "amount": 5})
    state[1].add({"date": "2020-12-24", "amount": 100})

    assert state[1].start == pd.to_datetime("2020-12-10", utc=True)
    assert state[1].end == pd.to_datetime("2020-12-24", utc=True)

    assert state[1]["2020-12-9"] == {"can_make": None, "kind": None, "amount": None}
    assert state[1]["2020-12-10"] == {"can_make": None, "kind": "elf", "amount": None}
    assert state[1][datetime(2020, 12, 22)] == {"can_make": "presents", "kind": "elf", "amount": 5}

    """
    Now we have configured the amount stream to interpolate linearly when between two states in a the amount stream
    """
    assert state[1][datetime(2020, 12, 23)] != {"can_make": "presents", "kind": "elf", "amount": 5}
    assert state[1][datetime(2020, 12, 23)] == {"can_make": "presents", "kind": "elf", "amount": 52}
    assert state[1][datetime(2020, 12, 23)]["can_make"] == "presents"


def test_df_inserts(conf_state_empty):
    state = conf_state_empty
    df = pd.DataFrame([
        {"id": 0, "date": "2020-12-10", "kind": "elf"},
        {"id": 1, "date": "2020-12-1", "kind": "grinch"},
        {"id": 1, "date": "2020-12-1", "can_make": "noise"},
        {"id": 1, "date": "2020-12-11", "can_make": "noise"},
        {"id": 0, "date": "2020-12-1", "can_make": "presents"},
        {"id": 0, "date": "2020-12-21", "amount": 4},
        {"id": 0, "date": "2020-12-22", "amount": 5},
        {"id": 0, "date": "2020-12-24", "amount": 100},
    ])

    state.include(df, "id", "date", columns=["kind", "can_make", "amount"])

    assert state.start == pd.to_datetime("2020-12-1", utc=True)
    assert state.end == pd.to_datetime("2020-12-24", utc=True)

    assert state[0].start == pd.to_datetime("2020-12-10", utc=True)
    assert state[0].end == pd.to_datetime("2020-12-24", utc=True)
    assert state[1].start == pd.to_datetime("2020-12-1", utc=True)
    assert state[1].end == pd.to_datetime("2020-12-11", utc=True)


