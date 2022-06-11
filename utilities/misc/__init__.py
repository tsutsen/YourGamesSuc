import numpy as np
import pandas as pd

def is_iterable(x):
    try:
        iter(x)
        return True
    except:
        return False


def empty_to_nan(x):
    if (is_iterable(x) and len(x) == 0) or (isinstance(x, list) and x[0] == '') or (x == ''):
        return np.nan
    else:
        return x


def extract_from_dict_series(series, dict_key):
    result = []
    for this in [x for x in series]:
        result_this = []
        if type(this) != float:
            for i in range(len(this)):
                result_this.append(this[i][dict_key])
        result.append(result_this)

    return result


def get_dict_key_by_value(val, d):
    for key, value in d.items():
        if val in value:
            return key
    return val


def get_dummy_df(df, param):
    result = df[['appid']]
    result = result.join(pd.get_dummies(df[param].apply(pd.Series).stack()).groupby(level=0).sum())
    result.columns = ['_'.join(x.lower().replace('-', ' ').split()) if type(x) == str else x for x in result.columns]

    return result
