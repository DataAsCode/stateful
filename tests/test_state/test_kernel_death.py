from datetime import datetime

from stateful import State
import pandas as pd
import numpy as np


def kernel_death_examples(user_id):
    yield pd.DataFrame([
        {
            'agreement_id': '401767b4-929a-4b58-b870-8dc44e2a3943',
            'agreement': 'started',
            "date": pd.to_datetime(datetime(2020, 2, 3), utc=True),
            'user_id': user_id,
            'payment': np.NaN,
            'customer_status': np.NaN,
            'travel': np.NaN,
            'primary_status': np.NaN
        },
        {
            'agreement_id': '401767b4-929a-4b58-b870-8dc44e2a3943',
            "date": pd.to_datetime(datetime(2020, 1, 3), utc=True),
            'agreement': 'ended',
            'user_id': user_id,
            'payment': np.NaN,
            'customer_status': np.NaN,
            'travel': np.NaN,
            'primary_status': np.NaN
        }
    ])
    # yield pd.DataFrame([
    #     {'agreement_id': '879b26a7-d85a-4c5d-a5ea-f84f87871f9f',
    #      'agreement': 'started',
    #      "date": pd.to_datetime(datetime(2020, 2, 3), utc=True),
    #      'user_id': user_id},
    #     {'agreement_id': '879b26a7-d85a-4c5d-a5ea-f84f87871f9f',
    #      "date": pd.to_datetime(datetime(2020, 1, 3), utc=True),
    #      'agreement': 'ended',
    #      'user_id': user_id}
    # ])


def test_kernel_death():
    user_id = 1
    for df in kernel_death_examples(user_id):
        state = State(primary_key="user_id", time_key="date")
        state.include(df,
                      primary_column="user_id",
                      columns=["agreement_id", "agreement", "payment", "customer_status", "travel", "primary_status"])

        def subscription_customer(state):
            if state["agreement"] == "started" and state["payment"] == "authorized":
                return "customer"

            return "not customer"

        def final_status(state):
            if state["primary_status"] == "customer" or state["travel"] == "enabled":
                return "customer"

            if len(state["primary_status"].before | state["travel"].before) == 0:
                return "registered"

            if "customer" in state.primary_status.after or "customer" in state.travel.after:
                return "paused"

            return "churned"

        space = state.space[user_id]
        space["primary_status"] = space[["agreement", "payment"]].apply(subscription_customer)
        space["customer_status"] = space[["primary_status", "travel"]].apply(final_status)
