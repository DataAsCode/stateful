def test_empty(simple_state_empty):
    state = simple_state_empty
    space = state.space[0]
    assert len(space) == 0
    assert space.empty == True

    space.add_stream(name="cool")

    stream = space["cool"]
    assert stream.empty == True

    stream.add("2000-01-01", 1)
    assert stream.empty == False

