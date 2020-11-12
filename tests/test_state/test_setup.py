def test_simple_setup(simple_state_empty):
    state = simple_state_empty
    assert not state
    assert len(state) == 0


def test_setup_with_configuration(conf_state_empty):
    state = conf_state_empty
    assert not state
    assert len(state) == 0

    state[0].add({"date": "2020-12-10", "kind": "elf", "amount": 5})

    assert state
    assert len(state) == 1

    # @no:format
    assert state[0]["amount"].configuration == {"dtype": "integer", "interpolation": "linear", 'on_dublicate': 'increment'}
    assert state[0]["kind"].configuration == {"dtype": "string", "interpolation": "floor", 'on_dublicate': 'increment'}
    assert state[0]["can_make"].configuration == {"dtype": "string", "interpolation": "floor", 'on_dublicate': 'increment'}
    # @do:format

    assert state[0]["kind"].dtype == "string"
    assert state[0]["kind"].interpolation == "floor"

    assert state[0]["amount"].dtype == "integer"
    assert state[0]["amount"].interpolation == "linear"