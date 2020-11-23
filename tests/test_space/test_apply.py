
def test_apply(numeric_state):
    state = numeric_state
    space = state.space[0]

    space["half"] = space["amount"].apply(lambda a: a / 2)
    assert space.keys == {"amount", "half"}
    assert space["2020-12-20"]["amount"] == -100
    assert space["2020-12-20"]["half"] == -50
    assert space["2020-12-21"]["half"] == -12
    assert space["2020-12-22"]["half"] == 25
    assert space["2020-12-24"]["half"] == 50


def test_multi_apply(numeric_state):
    state = numeric_state
    space = state.space[0]

    space["half"] = space["amount"].apply(lambda a: a / 2)
    half_half = space[["amount", "half"]].apply(lambda state: (state["amount"] + state["half"]))
    assert half_half["2020-12-20"] == -150
    assert int(half_half["2020-12-21"]) == -37
    assert half_half["2020-12-22"] == 75
    assert half_half["2020-12-24"] == 150
