from math import isclose
from functools import partial
from typing_extensions import assert_never

import numpy as np
import pandas as pd
import pytest
from sklearn.datasets import load_digits

from data_mastor import __version__
from data_mastor import mastor as ms


def test_version():
    assert __version__ == "0.1.0"


def load_data():
    digits = load_digits(as_frame=True, return_X_y=True)
    df = pd.concat([digits[0], digits[1]], axis=1)

    # digits = load_digits()
    # X = digits["data"]
    # y_2d = digits["target"]
    # y_2d = np.expand_dims(digits["target"], axis=1)
    # ret = np.concatenate([X, y_2d], axis=1)
    return df


@pytest.fixture
def digits():
    return load_data()


def test_subset_indices(digits):
    df = digits
    indices = ms.subset_indices(digits["target"], ratio=0.2)

    df_subset = df.loc[indices]

    vc = df["target"].value_counts() / len(df)
    vc_subset = df_subset["target"].value_counts() / len(df_subset)

    is_close_enough = partial(isclose, abs_tol=0.05)
    are_close = list(map(is_close_enough, vc.to_list(), vc_subset.tolist()))

    assert(False not in are_close)

if __name__ == "__main__":
    print("Name:", __name__)
    df = load_data()

    indices = ms.subset_indices(df["target"], ratio=0.2)

    df_subset = df.loc[indices]

    vc = df["target"].value_counts() / len(df)
    vc_subset = df_subset["target"].value_counts() / len(df_subset)
