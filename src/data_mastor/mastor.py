from __future__ import annotations

import pandas as pd
from sklearn.model_selection import ParameterGrid, train_test_split


def cv_results_df(cv_results, result_cols=None):

    if result_cols is None:
        result_cols = ["mean_test_score"]
    res = pd.DataFrame(cv_results)
    param_cols = [col for col in res.columns if col.startswith("param_")]
    ret = res[param_cols + result_cols].sort_values(result_cols[0], ascending=False)

    return ret


def stratified_data_subset(X, y, size_ratio=0.2, random_state=None):
    X_subset, _, y_subset, _ = train_test_split(
        X, y, test_size=1 - size_ratio, stratify=y, random_state=random_state
    )
    return X_subset, y_subset


def subset_indices(arr, ratio=0.2, random_state=0, **train_test_split_kwargs):
    index = range(arr.shape[0])
    index_train, index_test = train_test_split(
        index, test_size=ratio, random_state=random_state, **train_test_split_kwargs
    )
    return index_test


# TODO fix
def grid_search_no_cv(estimator, X, y, param_grid, scoring):
    for g in ParameterGrid(param_grid):
        estimator.set_params(**g)
        estimator.fit(X, y)

        best_score = 0
        if estimator.score > best_score:
            best_score = estimator.score

        return g
