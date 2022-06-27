import pandas as pd
from scipy import rand

from sklearn.model_selection import ParameterGrid, train_test_split


def cv_results_df(cv_results, result_cols=None):

    if result_cols is None:
        result_cols = ["mean_test_score"]
    res = pd.DataFrame(cv_results)
    param_cols = [col for col in res.columns if col.startswith("param_")]
    ret = res[param_cols + result_cols].sort_values(
        result_cols[0], ascending=False
    )

    return ret


def stratified_data_subset(X, y, size_ratio=0.2, random_state=None):
    X_subset, _, y_subset, _ = train_test_split(
        X, y, test_size=1 - size_ratio, stratify=y, random_state=random_state
    )
    return X_subset, y_subset


def subset_indices(arr, ratio=0.2, stratify=True, random_state=0):
    df = pd.DataFrame(arr)
    stratify = arr if stratify is True else None 
    index = df.index
    df_train, df_test = train_test_split(
        index, test_size=ratio, stratify=stratify, random_state=random_state
    )
    return df_test


# TODO fix
def grid_search_no_cv(estimator, X, y, param_grid, scoring):
    for g in ParameterGrid(param_grid):
        estimator.set_params(**g)
        estimator.fit(X, y)

        best_score = 0
        if estimator.score > best_score:
            best_score = estimator.score
            best_grid = g

        return g
