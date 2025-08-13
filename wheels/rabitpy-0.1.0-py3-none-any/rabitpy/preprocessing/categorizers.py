import pandas as pd


def qcut(data, q, suffix='_cat', labels=None, retbins=False, precision=3, duplicates='raise'):

    '''
    :param data: A DataFrame
    :param q: int, list, dictionary,
        Either the number of quantiles, a list of quantiles, or a dictionary with {column: n quantile}

    :param suffix: str
    :param labels: list or dictionary
        A list of labels corresponding to quantiles or a dictionary where each quantiles column name is the key

    :param retbins:
        Whether of not to return bins

    :param precision: int
        The precision at which to store and display the bins labels.

    :param duplicates: {default ‘raise’, ‘drop’}, optional
        If bin edges are not unique, raise ValueError or drop non-uniques.
    :return:
    '''

    if isinstance(q, dict):
        cols = q.keys()
        tmp = []
        for col in cols:
            try:
                if isinstance(labels, dict):
                    this_labels = labels.get(col, None)
                else:
                    this_labels = labels
                tmp.append(pd.qcut(data[col], q=q[col], labels=this_labels,
                                   precision=precision, duplicates=duplicates))
            except KeyError:
                raise
        tmp = pd.concat(tmp, axis=1).add_suffix(suffix=suffix)
        df =data.merge(tmp, left_index=True, right_index=True)
    else:
        cols = data.select_dtypes(include='number')
        tmp = data[cols].apply(pd.qcut, axis=1, q=q, labels=labels, precision=precision, duplicates=duplicates)
        df = tmp.add_suffix(suffix=suffix)

    return df

def remap(data, cats):

    return None
