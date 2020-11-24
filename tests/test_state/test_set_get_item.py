def test_set_get_item(stateful_state):
    def is_cool(state):
        return "cool" if state == "cool" or state == "uber cool" else "not cool"

    def cool_master(state):
        if state["is_cool"] == "cool" and state.payment == "mastercard":
            return "cool master"
        if state['agreement'] == "uber cool":
            return "better"
        return "bad"

    state = stateful_state
    state["is_cool"] = state["agreement"].apply(is_cool)

    for space in state.all_spaces.values():
        assert set(space.keys) == set(state.keys) == {"is_cool", "agreement", "amount", "payment", "weird_stream"}

    assert state.space[0]["2020-12-21"]["is_cool"] == "cool"
    assert state.space[1]["2020-12-25"]["is_cool"] == "cool"
    assert state.space[2]["2020-12-25"]["is_cool"] == "not cool"
    assert state.space[3]["2020-12-25"]["is_cool"] == "cool"
    assert state.space[7]["2020-12-25"]["is_cool"] == "not cool"

    state["cool_master"] = state[["agreement", "is_cool", "payment"]].apply(cool_master)

    assert state.space[0]["2020-12-21"]["cool_master"] == "bad"
    assert state.space[7]["2020-12-24"]["cool_master"] == "bad"
    assert state.space[3]["2020-12-24"]["cool_master"] == "better"
