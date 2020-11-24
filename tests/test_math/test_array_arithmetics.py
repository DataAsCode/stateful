

def test_all_function(numeric_state):
    state = numeric_state

    state["subtracted"] = state["amount"] - state["amount"]

    space = state.space[1]
    all_events = space.all()

    assert len(all_events) == 3