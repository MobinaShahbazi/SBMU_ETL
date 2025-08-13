import pandas as pd
import re
from math import inf
from khayyam import JalaliDate
import numpy as np

"""
df: pandas dataframe 
metadata: metadata in form of a pandas dataframe or dictionary
_s: a column of df pandas series
_meta: a row of metadata
"""


# %% datetime
def _datetime_validator(_s, _meta):
    # convert data to datetime
    _s = _s.apply(lambda x: pd.to_datetime(x, format=_meta.iloc[0].get('format', None), errors='coerce'))
    return _s


def _datetime_limiter(_s, _meta):
    # change data outside defined range_min and range_max to pd.NaT
    try:
        # TODO check format with pouya
        _meta['datetime_max'] = _meta.apply(lambda x: pd.to_datetime(x['datetime_max'], format=x.get('format', None),
                                                                     errors='coerce'), axis=1)
    except KeyError:
        _meta['datetime_max'] = None
    try:
        _meta['datetime_min'] = _meta.apply(lambda x: pd.to_datetime(x['datetime_min'], format=x.get('format', None),
                                                                     errors='coerce'), axis=1)
    except KeyError:
        _meta['datetime_min'] = None
    _meta['datetime_max'].fillna(pd.Timestamp.max, inplace=True)
    _meta['datetime_min'].fillna(pd.Timestamp.min, inplace=True)

    iswithinlimits = _s.apply(
        lambda x: (x > _meta.iloc[0]['datetime_min']) & (x < _meta.iloc[0]['datetime_max']) if x else False)
    _s[~iswithinlimits] = pd.NaT
    return _s


def _datetime_converter(_s, _meta):
    # convert from datetime to jalalidate
    if _meta.iloc[0].get('convert', None):
        _s = _s.apply(lambda x: pd.NaT if pd.isna(x) else to_jalalidate(x))
    return _s


def datetime_handler(_s, _meta):
    _s = _datetime_validator(_s, _meta)
    _s = _datetime_limiter(_s, _meta)
    _s = _datetime_converter(_s, _meta)
    return _s


# %% jalalidate
def inferdate_format(x):
    # try to find simple datetime formats
    try:
        sep = re.findall(r'([-/., ])', str(x))[0]
    except IndexError:
        sep = ''
    return f'%Y{sep}%m{sep}%d'


def to_jalalidate(x, _format=None, errors='coerce'):
    # a wrapper for JalaliDate.strptime function in order to add how to handle errors
    try:
        if _format is not None:
            return JalaliDate.strptime(x, _format)
        else:
            return JalaliDate(x)
    except ValueError:
        if errors == 'coerce':
            return pd.NaT
    except TypeError:
        if errors == 'coerce':
            return pd.NaT


def _jalalidate_validator(_s, _meta):
    # convert data to jalalidate
    _format = _meta.iloc[0].get('format')
    if len(_s.loc[_s.notna()]) > 0:
        _format = _format if _format else inferdate_format(_s.loc[_s.notna()].iloc[0])
    else:
        _format = None
    _s = _s.apply(lambda x: to_jalalidate(str(x), _format))
    return _s


def _jalalidate_limiter(_s, _meta):
    # change data outside defined range_min and range_max to pd.NaT
    _format = _meta.iloc[0].get('format')
    try:
        # TODO check format with pouya
        _format = _format if _format else inferdate_format(_meta.iloc[0]['jalalidate_max'])
        _meta['jalalidate_max'] = _meta.apply(lambda x: to_jalalidate(str(x['jalalidate_max']), _format), axis=1)
    except KeyError:
        _meta['jalalidate_max'] = None
    try:
        _format = _format if _format else inferdate_format(_meta.iloc[0]['jalalidate_min'])
        _meta['jalalidate_min'] = _meta.apply(lambda x: to_jalalidate(str(x['jalalidate_min']), _format), axis=1)
    except KeyError:
        _meta['jalalidate_min'] = None
    _meta['jalalidate_max'].fillna(JalaliDate.max, inplace=True)
    _meta['jalalidate_min'].fillna(JalaliDate.min, inplace=True)

    iswithinlimits = _s.apply(
        lambda x: (x > _meta.iloc[0]['jalalidate_min']) & (x < _meta.iloc[0]['jalalidate_max']) if pd.notna(x) else False)
    _s[~iswithinlimits] = pd.NaT
    return _s


def _jalalidate_converter(_s, _meta):
    # convert from jalalidate to datetime
    if _meta.iloc[0].get('convert', None):
        _s = _s.apply(lambda x: pd.NaT if pd.isna(x) else x.todate())
    return _s


def jalalidate_handler(_s, _meta):
    _s = _jalalidate_validator(_s, _meta)
    _s = _jalalidate_limiter(_s, _meta)
    _s = _jalalidate_converter(_s, _meta)
    return _s


# %% numeric
def to_float(x):
    try:
        return float(x)
    except ValueError:
        return None


def _numeric_validator(_s, _meta):
    # convert data to numeric
    return pd.to_numeric(_s, errors='coerce')


def _numeric_extractor(_s, _meta):
    if _meta.iloc[0].get('extract', None):
        # TODO change regex to cover float numbers
        _s = _s.apply(lambda x: ''.join(re.findall('\d+', str(x))))
    return _s


def _numeric_limiter(_s, _meta):
    # change data outside defined range_min and range_max to None

    try:
        _meta['numeric_max'] = pd.to_numeric(_meta['numeric_max'], errors='coerce')
    except KeyError:
        _meta['numeric_max'] = None
    try:
        _meta['numeric_min'] = pd.to_numeric(_meta['numeric_min'], errors='coerce')
    except KeyError:
        _meta['numeric_min'] = None
    _meta['numeric_max'].fillna(inf, inplace=True)
    _meta['numeric_min'].fillna(-inf, inplace=True)

    _var = _s.name
    iswithinlimits = (_meta.set_index('fldCode').loc[_var, 'numeric_min'] <= _s) & \
                     (_meta.set_index('fldCode').loc[_var, 'numeric_max'] > _s)
    _s[~iswithinlimits] = None
    return _s


def numeric_handler(_s, _meta):
    _s = _numeric_extractor(_s, _meta)
    _s = _numeric_validator(_s, _meta)
    _s = _numeric_limiter(_s, _meta)

    return _s


# %% string
def _string_validator(_s, _meta):
    if _meta.iloc[0].get('match', None):
        _s.loc[_s.str.match(_meta.iloc[0]['match']) != True] = None
    if _meta.iloc[0].get('contains', None):
        _s.loc[_s.str.contains(_meta.iloc[0]['contains']) != True] = None
    if _meta.iloc[0].get('startswith', None):
        _s.loc[_s.str.startswith(_meta.iloc[0]['startswith']) != True] = None
    if _meta.iloc[0].get('endswith', None):
        _s.loc[_s.str.endswith(_meta.iloc[0]['endswith']) != True] = None
    _s.loc[_s.notna()] = _s.loc[_s.notna()].apply(lambda x: str(x))
    return _s


def _string_extractor(_s, _meta):
    if _meta.iloc[0].get('format', None):
        _format = _meta.iloc[0]['format']
        _s = _s.apply(lambda x: None if (len(re.findall(_format, x)) == 0) else ''.join(re.findall(_format, x)))
    return _s


# a = re.match(_format, x)
# if a is None:
# 	return None
# else:
# 	return x[re.search(_format, x).span()[0]: re.search(_format, x).span()[1]]


def _string_limiter(_s, _meta):
    # change data the length of which is outside defined range_min and range_max to None
    try:
        _meta['string_max'] = pd.to_numeric(_meta['string_max'])
    except KeyError:
        _meta['string_max'] = None
    try:
        _meta['string_min'] = pd.to_numeric(_meta['string_min'])
    except KeyError:
        _meta['string_min'] = None
    _meta['string_max'].fillna(inf, inplace=True)
    _meta['string_min'].fillna(-inf, inplace=True)

    _var = _s.name
    iswithinlimits = (_meta.set_index('fldCode').loc[_var, 'string_min'] <=
                      _s.apply(lambda x: None if pd.isna(x) else len(x))) & \
                     (_meta.set_index('fldCode').loc[_var, 'string_max'] >
                      _s.apply(lambda x: None if pd.isna(x) else len(x)))
    _s[~iswithinlimits] = None
    return _s


def string_handler(_s, _meta):
    _s = _string_validator(_s, _meta)
    _s = _string_extractor(_s, _meta)
    _s = _string_limiter(_s, _meta)
    return _s


# %% category
def _category_validator(_s, _meta):
    # convert data to pandas categorical

    if _meta.iloc[0].get('categories', None):
        tmp = pd.Categorical(_s, categories=_meta.iloc[0]['categories'], ordered=_meta.iloc[0].get('ordered', False))
        _s = tmp
    else:
        tmp = pd.Categorical(_s, categories=list(filter(pd.notna, _s.unique())),
                             ordered=_meta.iloc[0].get('ordered', False))
        _s = tmp
    return _s


def category_handler(_s, _meta):
    _s = _category_validator(_s, _meta)
    return _s


# %% bool
def _bool_convertor(_s, _meta):
    if _meta.iloc[0].get('true'):
        _s = _s.apply(lambda x:
                      True if x == _meta.iloc[0]['true'] else False if x == _meta.iloc[0]['false'] else None)
    return _s


def bool_handler(_s, _meta):
    _s = _bool_convertor(_s, _meta)
    return _s


# %%
def metadata_handler(metadata):
    # TODO metadata type
    if isinstance(metadata, dict):
        metadata = pd.DataFrame(metadata)
    metadata = metadata.fillna(np.nan).astype(object).replace([np.nan], [None])
    return metadata


def handler(data, meta):
    df = data.copy()
    metadata = meta.copy()
    metadata = metadata_handler(metadata)
    df = df.fillna(np.nan).astype(object).replace([np.nan], [None])
    for type_name in ['datetime', 'jalalidate', 'numeric', 'str', 'category', 'bool']:
        # TODO check files format
        cols = metadata.loc[metadata['dType'] == type_name, 'fldCode'].to_list()
        if cols:
            if type_name == 'datetime':
                try:
                    df[cols] = df[cols].apply(lambda x: datetime_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
                except Exception:
                    print(e)
                    print('handler datetime')
            if type_name == 'jalalidate':
                try:
                    df[cols] = df[cols].apply(lambda x: jalalidate_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
                except Exception as e:
                    print(e)
                    print('handler jalalidate')
            elif type_name == 'numeric':
                try:
                    df[cols] = df[cols].apply(lambda x: numeric_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
                except Exception as e:
                    print(e)
                    print('handler numeric')
            elif type_name == 'str':
                try:
                    df[cols] = df[cols].apply(lambda x: string_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
                except Exception as e:
                    print(e)
                    print('handler str')
            # elif type_name == 'category':
            # 	df[cols] = df[cols].apply(lambda x: category_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
            elif type_name == 'bool':
                try:
                    df[cols] = df[cols].apply(lambda x: bool_handler(x, metadata.loc[metadata['fldCode'] == x.name]))
                except Exception as e:
                    print(e)
                    print('handler bool')
    return df
