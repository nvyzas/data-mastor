from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from collections.abc import Sequence, Iterable
    from numpy.typing import ArrayLike

import json

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from mlflow.utils.mlflow_tags import MLFLOW_PARENT_RUN_ID, MLFLOW_RUN_NAME
from ray import tune
from ray.tune.search.sample import Categorical, Domain, Quantized
from sklearn.model_selection import ParameterGrid, train_test_split

from . import pyutils as pu


def cv_results_df(cv_results, result_cols=None):
    if result_cols is None:
        result_cols = ["mean_test_score"]
    res = pd.DataFrame(cv_results)
    param_cols = [col for col in res.columns if col.startswith("param_")]
    ret = res[param_cols + result_cols].sort_values(result_cols[0], ascending=False)

    return ret


# %% scikit-learn
def stratified_data_subset(X, y, size_ratio=0.2, random_state=None):
    X_subset, _, y_subset, _ = train_test_split(
        X, y, test_size=1 - size_ratio, stratify=y, random_state=random_state
    )
    return X_subset, y_subset


def subset_indices(arr, ratio=0.2, random_state=0, **train_test_split_kwargs):
    index = range(arr.shape[0])
    index_train, index_test = train_test_split(
        index,
        test_size=ratio,
        stratify=arr,
        random_state=random_state,
        **train_test_split_kwargs,
    )
    return index_test


def data_subsets(
    X,
    y,
    train_ratio: float | int | None = 0.8,
    test_ratio: float | int | None = None,
    val_ratio: float | int | None = None,
    ratio_factor: float = 1.0,
    random_state=None,
    stratify=None,  # TODO use a custom array to stratify
):
    ratios = []
    sets = []
    if train_ratio:
        train_ratio = train_ratio * ratio_factor
        ratios.append(train_ratio)
        sets.append("train")
    if test_ratio:
        test_ratio = test_ratio * ratio_factor
        ratios.append(test_ratio)
        sets.append("test")
    if val_ratio:
        val_ratio = val_ratio * ratio_factor
        ratios.append(val_ratio)
        sets.append("val")

    data = {}

    num_classes = len(np.unique(y))

    X_left, y_left, ratio_left, ratio_used = X, y, 1.0, 1.0
    for r, s in zip(ratios, sets):
        stratify = y_left
        r_adjusted = r / ratio_left
        num_left_examples = (1.0 - r_adjusted) * len(X_left)
        if r_adjusted >= 1.0 or num_left_examples < num_classes:
            print("Re-adjusting r", r_adjusted)
            print(f"{num_left_examples=}")
            r_adjusted = num_classes

        # alternative way if no indices are needed
        X_train, X_left, y_train, y_left = train_test_split(
            X_left,
            y_left,
            train_size=r_adjusted,
            stratify=stratify,
            random_state=random_state,
        )

        # sss = StratifiedShuffleSplit(
        #     n_splits=1, train_size=r / ratio_left, random_state=random_state
        # )
        # index_train, index_left = next(sss.split(X_left, y_left))
        # X_train = X_left[index_train]
        # y_train = y_left[index_train]
        # X_left = X_left[index_left]
        # y_left = y_left[index_left]

        ratio_left = ratio_left - r
        if r_adjusted >= 1.0:
            X_train = np.concatenate([X_train, X_left])
            y_train = np.concatenate([y_train, y_left])

        # data["index_" + s] = index_train
        data["X_" + s] = X_train
        data["y_" + s] = y_train

    return data


def plot_class_distributions(
    arrs: Iterable[ArrayLike],
    names: Sequence[str] = ("train", "test", "val"),
    perc=True,
) -> None:
    dists = []
    for i, arr in enumerate(arrs):
        arr_series = pd.Series(arr, name=names[i])
        dist = arr_series.groupby(arr_series).count()
        if perc:
            dist = dist / dist.sum()
        dists.append(dist)

    pd.concat(dists, axis=1).plot.bar()


# plot_class_distributions(
#     [data["y_train"], data["y_test"], data["y_val"]], perc=True
# )


# mlflow
MLFLOW_LOGGER_TAGNAME = "mlflow_logger"
RAY_MLFLOW_CALLBACK_LOGGER = "ray-MLFlowLoggerCallback"
RAY_MLFLOW_MIXIN_LOGGER = "ray-mlflow_mixin"

MLFLOW_RUNGROUP_TAGNAME = "mlflow_rungroup"


def find_runs(
    run_name_contains=None,
    exclude_child_runs=False,
    in_latest_run_group=False,
    **search_runs_kwargs,
):
    df = mlflow.search_runs(**search_runs_kwargs)

    col = f"tags.{MLFLOW_RUN_NAME}"
    if run_name_contains and (col in df.columns):
        df = df[df[col].str.contains(run_name_contains)]
    # filter_string = f"tags.`{MLFLOW_RUN_NAME}` = '{run_name}'"

    col = f"tags.{MLFLOW_PARENT_RUN_ID}"
    if exclude_child_runs and (col in df.columns):
        df = df[df[col].isna()]

    col = f"tags.{MLFLOW_RUNGROUP_TAGNAME}"
    if in_latest_run_group and (col in df.columns):
        latest_run_group = df[col].sort_values(ascending=False).iloc[0]
        df = df[df[col] == latest_run_group]
    # filter_string = f'tags.{MLFLOW_RUNGROUP_TAGNAME} = "{latest_run_group}"'

    return df


def assimilate_ray_runs(found_runs_df):
    RAY_MLFLOW_MASTOR_LOGGER = "data-mastor-combine"
    RAY_MLFLOW_TRIAL_TAGNAME = "trial_name"

    df = found_runs_df
    df_cback = df[df[f"tags.{MLFLOW_LOGGER_TAGNAME}"] == RAY_MLFLOW_CALLBACK_LOGGER]
    df_mixin = df[~(df[f"tags.{MLFLOW_LOGGER_TAGNAME}"] == RAY_MLFLOW_CALLBACK_LOGGER)]
    # df_mixin = df[
    #     df[f"tags.{MLFLOW_LOGGER_TAGNAME}"] == RAY_MLFLOW_MIXIN_LOGGER
    # ]

    mixin_run_ids = df_mixin["run_id"].tolist()
    cback_run_ids = df_cback["run_id"].tolist()
    # print(f"{mixin_run_ids=}")
    # print(f"{cback_run_ids=}")

    for mixin_run_id, cback_run_id in zip(mixin_run_ids, cback_run_ids):
        mixin_run = mlflow.get_run(mixin_run_id)
        cback_run = mlflow.get_run(cback_run_id)

        with mlflow.start_run(run_id=mixin_run_id) as combined_run:
            mlflow.log_metrics(pu.prefix_dict_keys(cback_run.data.metrics, "ray_"))

            mlflow.log_params(pu.prefix_dict_keys(cback_run.data.params, "ray_conf_"))

            tags_to_set = {
                k: v
                for k, v in cback_run.data.tags.items()
                if k
                in {
                    MLFLOW_RUNGROUP_TAGNAME,
                    RAY_MLFLOW_TRIAL_TAGNAME,
                    MLFLOW_RUN_NAME,
                }
            }
            tags_to_set[MLFLOW_LOGGER_TAGNAME] = RAY_MLFLOW_MASTOR_LOGGER
            # tags_to_set[MLFLOW_RUN_NAME]=mixin_run_id
            mlflow.set_tags(tags_to_set)

    return mixin_run_ids, cback_run_ids


def create_tune_summary_run(config_space, run_name=""):
    if not run_name:
        run_name = "Ray Tune Rungroup Summary"

    config_space_dict = traverse_nested_tune_space(config_space)
    config_space_json = json.dumps(config_space_dict, indent=4)
    with mlflow.start_run(run_name=run_name) as tune_summary_run:
        # mlflow.log_param("ray_conf", config_space_dict)
        mlflow.log_dict(config_space_json, "config_space.json")
    tune_summary_run_id = tune_summary_run.info.run_id

    return tune_summary_run_id


def adopt_runs(children_run_ids, parent_run_id):
    for run_id in children_run_ids:
        print("Adopting run with id:", run_id)
        with mlflow.start_run(run_id=run_id) as run:
            mlflow.set_tag(f"{MLFLOW_PARENT_RUN_ID}", parent_run_id)


def organize_latest_ray_runs(
    experiment_name, config_space, run_name="", delete_stray_runs=False
):
    finds = find_runs(
        in_latest_run_group=True,
        experiment_names=[experiment_name],
    )
    mixin_run_ids, cback_run_ids = assimilate_ray_runs(finds)
    print(mixin_run_ids)
    tune_summary_run_id = create_tune_summary_run(config_space, run_name=run_name)

    adopt_runs(mixin_run_ids, tune_summary_run_id)

    if delete_stray_runs:
        for run_id in cback_run_ids:
            mlflow.delete_run(run_id)


def print_auto_logged_info(r):
    """
    print_auto_logged_info(mlflow.get_run(run_id=run.info.run_id))
    """
    tags = {k: v for k, v in r.data.tags.items() if not k.startswith("mlflow.")}
    artifacts = [f.path for f in MlflowClient().list_artifacts(r.info.run_id, "model")]
    print(f"run_id: {r.info.run_id}")
    print(f"artifacts: {artifacts}")
    print(f"params: {r.data.params}")
    print(f"metrics: {r.data.metrics}")
    print(f"tags: {tags}")


def end_active_run():
    if mlflow.active_run() is not None:
        mlflow.end_run()
        print("Ended active run")
    else:
        print("No active run to end")


# %% ray
def describe_as_dict(samfuncret: Domain):
    desc = {}
    print(type(samfuncret))
    print(samfuncret)
    if isinstance(samfuncret, Domain):
        print(samfuncret.sampler.__class__)
        if isinstance(samfuncret.sampler, Quantized):
            desc["quantization"] = f'q = {samfuncret.get_sampler().__dict__["q"]}'
            sampler = samfuncret.get_sampler().__dict__["sampler"]
            desc["sampler"] = sampler.__str__()

        else:
            sampler = samfuncret.sampler
            desc["sampler"] = sampler.__str__()

        if desc["sampler"] == "Normal":
            desc["distribution_parameters"] = str(sampler.__dict__)

        desc["domain"] = samfuncret.domain_str
    else:
        raise ValueError("Input argument must be an instance of tune.sample.Domain")

    return desc


def sample_tune_space(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for key in obj:
            new_val = sample_tune_space(obj[key])
            new_obj[key] = new_val

        return new_obj
    elif isinstance(obj, list):
        new_obj = []
        for el in obj:
            new_el = sample_tune_space(el)
            new_obj.append(new_el)

        return new_obj

    elif isinstance(obj, tuple):
        new_obj_list = list(obj)
        new_obj = tuple(sample_tune_space(new_obj_list))
        return new_obj

    elif isinstance(obj, Categorical):
        sampled_obj = obj.sample()

        return sample_tune_space(sampled_obj)

    elif isinstance(obj, Domain):
        sample = obj.sample()
        return sample
    else:
        return obj


def traverse_nested_tune_space(obj, func=lambda x: print(x)):
    if isinstance(obj, dict):
        new_obj = {}
        for key in obj:
            new_val = traverse_nested_tune_space(obj[key])
            new_obj[key] = new_val

        return new_obj

    elif isinstance(obj, list):
        new_obj = []
        for el in obj:
            new_el = traverse_nested_tune_space(el)
            new_obj.append(new_el)

        return new_obj

    elif isinstance(obj, tuple):
        new_obj_list = list(obj)
        new_obj = tuple(traverse_nested_tune_space(new_obj_list))
        return new_obj

    elif isinstance(obj, Categorical):
        new_obj = {}
        new_obj["sampler"] = "tune.choice"

        new_el_list = []
        for el in obj:
            new_el = traverse_nested_tune_space(el)
            new_el_list.append(new_el)

        new_obj["categories"] = new_el_list
        return new_obj

    # a domain (other than Categorical)
    elif isinstance(obj, Domain):
        # obj_dict = {key:str(val) for key,val in obj.__dict__.items()}
        obj_dict = describe_as_dict(obj)

        return obj_dict

    else:
        return str(obj)
