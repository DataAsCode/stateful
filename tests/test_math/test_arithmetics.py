def test_add(conf_state_empty):
    state = conf_state_empty
    state.space[0].add({"date": "2020-01-01", "amount": 0})
    state.space[0].add({"date": "2020-01-03", "amount": 5})

    result = state.space[0]["amount"] + state.space[0]["amount"]
    assert result.dtype == state.space[0]["amount"].dtype
    assert result.dtype == state.space[0]["amount"].dtype == "integer"

    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10

    state.space[0].add({"date": "2019-12-30", "amount": -10})

    result = state.space[0]["amount"] + state.space[0]["amount"]
    assert result["2019-12-30"] == -20
    assert result["2019-12-31"] == -10
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10

    state.space[0].add({"date": "2020-01-5", "amount": 20})

    result = state.space[0]["amount"] + state.space[0]["amount"]
    assert result["2019-12-30"] == -20
    assert result["2019-12-31"] == -10
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 5
    assert result["2020-01-03"] == 10
    assert result["2020-01-04"] == 25
    assert result["2020-01-05"] == 40


def test_subtract(conf_state_empty):
    state = conf_state_empty
    state.space[0].add({"date": "2020-01-01", "amount": 0})
    state.space[0].add({"date": "2020-01-03", "amount": 5})

    result = state.space[0]["amount"] - state.space[0]["amount"]
    assert result.dtype == state.space[0]["amount"].dtype
    
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0

    state.space[0].add({"date": "2019-12-30", "amount": -10})

    result = state.space[0]["amount"] - state.space[0]["amount"]
    assert result["2019-12-30"] == 0
    assert result["2019-12-31"] == 0
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0

    state.space[0].add({"date": "2020-01-5", "amount": 20})

    result = state.space[0]["amount"] - state.space[0]["amount"]
    assert result["2019-12-30"] == 0
    assert result["2019-12-31"] == 0
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 0
    assert result["2020-01-03"] == 0
    assert result["2020-01-04"] == 0  # User 1 is mid way between 5 and 20 which becomes 0 - ((5 + 20) / 2) = int(12.5) = 12
    assert result["2020-01-05"] == 0


def test_multiplication(conf_state_empty):
    state = conf_state_empty
    state.space[0].add({"date": "2020-01-01", "amount": 0})
    state.space[0].add({"date": "2020-01-03", "amount": 5})
    state.space[1].add({"date": "2020-01-01", "amount": 0})
    state.space[1].add({"date": "2020-01-03", "amount": 5})

    result = state.space[0]["amount"] * state.space[0]["amount"]
    assert result.dtype == state.space[0]["amount"].dtype
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25

    state.space[0].add({"date": "2019-12-30", "amount": -10})

    result = state.space[0]["amount"] * state.space[0]["amount"]
    assert result["2019-12-30"] == 100
    assert result["2019-12-31"] == 25
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25

    state.space[0].add({"date": "2020-01-5", "amount": 20})

    result = state.space[0]["amount"] * state.space[0]["amount"]
    assert result["2019-12-30"] == 100
    assert result["2019-12-31"] == 25
    assert result["2020-01-01"] == 0
    assert result["2020-01-02"] == 6
    assert result["2020-01-03"] == 25
    assert result["2020-01-04"] == 156
    assert result["2020-01-05"] == 400
