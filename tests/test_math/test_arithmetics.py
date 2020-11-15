def test_add(conf_state_empty):
    state = conf_state_empty
    state[0].add({"date": "2020-01-01", "amount": 0})
    state[0].add({"date": "2020-01-03", "amount": 5})
    state[1].add({"date": "2020-01-01", "amount": 0})
    state[1].add({"date": "2020-01-03", "amount": 5})

    result = state[0].stream["amount"] + state[1].stream["amount"]
    assert result.dtype == state[0].stream["amount"].dtype
    assert result.history.dtype == state[0].stream["amount"].history.dtype == "float32"

    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10

    state[0].add({"date": "2019-12-30", "amount": -10})

    result = state[0].stream["amount"] + state[1].stream["amount"]
    assert result["2019-12-30"] == -10
    assert result["2019-12-31"] == -5
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10

    state[1].add({"date": "2020-01-5", "amount": 20})

    result = state[0].stream["amount"] + state[1].stream["amount"]
    assert result["2019-12-30"] == -10
    assert result["2019-12-31"] == -5
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10
    assert result["2020-01-04"] == 12
    assert result["2020-01-05"] == 20


def test_subtract(conf_state_empty):
    state = conf_state_empty
    state[0].add({"date": "2020-01-01", "amount": 0})
    state[0].add({"date": "2020-01-03", "amount": 5})
    state[1].add({"date": "2020-01-01", "amount": 0})
    state[1].add({"date": "2020-01-03", "amount": 5})

    result = state[0].stream["amount"] - state[1].stream["amount"]
    assert result.dtype == state[0].stream["amount"].dtype
    assert result.history.dtype == state[0].stream["amount"].history.dtype == "float32"

    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0

    state[0].add({"date": "2019-12-30", "amount": -10})

    result = state[0].stream["amount"] - state[1].stream["amount"]
    assert result["2019-12-30"] == -10
    assert result["2019-12-31"] == -5
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0

    state[1].add({"date": "2020-01-5", "amount": 20})

    result = state[0].stream["amount"] - state[1].stream["amount"]
    assert result["2019-12-30"] == -10
    assert result["2019-12-31"] == -5
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0
    assert result["2020-01-04"] == -12  # User 1 is mid way between 5 and 20 which becomes 0 - ((5 + 20) / 2) = int(12.5) = 12
    assert result["2020-01-05"] == -20


def test_multiplication(conf_state_empty):
    state = conf_state_empty
    state[0].add({"date": "2020-01-01", "amount": 0})
    state[0].add({"date": "2020-01-03", "amount": 5})
    state[1].add({"date": "2020-01-01", "amount": 0})
    state[1].add({"date": "2020-01-03", "amount": 5})

    result = state[0].stream["amount"] * state[1].stream["amount"]
    assert result.dtype == state[0].stream["amount"].dtype
    assert result.history.dtype == state[0].stream["amount"].history.dtype == "float32"
    print(state[0].stream["amount"]["2020-01-02"])
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25

    state[0].add({"date": "2019-12-30", "amount": -10})

    result = state[0].stream["amount"] * state[1].stream["amount"]
    assert result["2019-12-30"] == 0
    assert result["2019-12-31"] == 0
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25

    state[1].add({"date": "2020-01-5", "amount": 20})

    result = state[0].stream["amount"] * state[1].stream["amount"]
    assert result["2019-12-30"] == 0
    assert result["2019-12-31"] == 0
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25
    assert result["2020-01-04"] == 0
    assert result["2020-01-05"] == 0
