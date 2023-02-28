# working with dicts


def prefix_dict_keys(d, prefix):
    new_d = {}
    for key, val in d.items():
        if isinstance(key, str):
            new_key = prefix + key
            new_d[new_key] = val
        else:
            new_d[key] = val

    return new_d


# misc
def printvar(var):
    print(f"{var=}")
