# SUMIT report procedures

import numpy as np
import pandas as pd
from rabitpy.preprocessing import Metadata


# from pandas.api.types import is_datetime64_any_dtype as is_datetime

# TODO: convert outputs to exit codes
# TODO: The returns here should be converted to exception and handled in the main code
def check_dataframe_validity(df, columns=[]):
    if not isinstance(df, pd.DataFrame):
        return "The input variable is not a DataFrame!"
    if df.empty:
        return "The input DataFrame is empty!"
    if not set(columns).issubset(df.columns):
        return "One or more specified column names are not in the DataFrame column list!"
    return "success!"


class DescriptiveReporter:
    """Common base class for all Reports"""
    objCount = 0

    def __init__(self, df, dfmeta=None, missing_code=np.nan, skipped_code=-9999, **kwargs):

        '''
        :param df:
        :param dfmeta:
        '''

        self.ReportType = ""
        self.df = df
        self.md = dfmeta
        self.missing_code = missing_code

        self.nanfill = kwargs.get('nanfill', missing_code)

        if not isinstance(skipped_code, float) and not isinstance(skipped_code, int):
            try:
                self.skipped_code = float(skipped_code)
            except ValueError:
                raise (f'Skipped observation code: {skipped_code} could not be represented as a numeric type.')
        else:
            self.skipped_code = skipped_code

        DescriptiveReporter.objCount += 1
        validity = check_dataframe_validity(df)

        if check_dataframe_validity(dfmeta) == 'success!':
            self.infer_types = False
        else:
            self.infer_types = True

        if validity != "success!":
            raise Exception(validity)

    def type(self, ImputationType):
        pass

    def get_df(self):
        return self.df

    def report_numeric_columns(self):
        """Returns a description of numeric columns of the dataframe"""
        df2 = self.df.copy()

        if self.infer_types:
            metadata = Metadata.metadata(self.df)
            int_columns = metadata.find_int_columns()
            float_columns = metadata.find_float_columns()
            numCols = int_columns + float_columns
        else:
            numCols = self.md.loc[self.md['dType'] == 'numeric', 'fldCode'].to_list()
        numCols = list(set(numCols).intersection(self.df.columns))
        res = pd.DataFrame()
        if not df2[numCols].empty:
            # TODO: data types should be set and cleaned before getting to this point.
            #  I have added this here but should be moved elsewhere
            df2[numCols] = df2[numCols].apply(lambda x: pd.to_numeric(x, errors='coerce'))

            # Get missing values report
            mrep = self._get_missing_stats(_df=df2)

            # Merge with descriptive report and return
            res = df2[numCols].astype('float64').describe(include='all').transpose() \
                .merge(mrep, left_index=True, right_index=True)

        return res

    def report_datetime_columns(self):
        """Returns a description of datetime columns of the dataframe"""

        if self.infer_types:
            metadata = Metadata.metadata(self.df)
            dtCols = list(set(metadata.find_datetime_columns()).intersection(self.df.columns))
            if dtCols == []:
                return
        else:
            dtCols = self.md.loc[self.md['dType'] == 'datetime', 'fldCode'].to_list()

        self.df[dtCols] = self.df[dtCols].apply(pd.to_datetime)
        # print(self.df[datetime_columns].describe(datetime_is_numeric=True))
        res = self.df[dtCols].describe(datetime_is_numeric=True)

        # nulls_percent = []
        # for col in dtCols:
        #     nulls_percent.append((((self.df[col].isnull().sum()) * 100) / len(self.df.index)))
        # res.loc['missings%'] = nulls_percent
        # nulls = []
        #
        # for col in dtCols:
        #     nulls.append((self.df[col].isnull().sum()))
        # res.loc['missings_count'] = nulls
        # res = res.transpose()

        return res

    def report_categorical_columns(self):
        """:param Returns a description of categorical columns of the dataframe"""

        # Infer types or read cv
        if self.infer_types:
            metadata = Metadata.metadata(self.df)
            cat_columns = metadata.find_categorical_columns()
            bool_columns = metadata.find_boolean_columns()
            # time_columns = self.find_datetime_columns()
            sum_columns = cat_columns + bool_columns
        else:
            sum_columns = self.md.loc[(self.md['dType'] == 'category') |
                                      (self.md['dType'] == 'bool') |
                                      ((self.md['dType'] == 'str') & (self.md['elementType'] != 'html')), 'fldCode'] \
                .drop_duplicates() \
                .to_list()
        sum_columns = list(set(sum_columns).intersection(self.df.columns))
        res = pd.DataFrame()

        for col in sum_columns:

            # for each column check if datatype is 'category' or 'bool'
            # then get value_counts for each column in counts and frequency form and concat the two

            if all(self.md.loc[self.md['fldCode'] == col, 'dType'] == 'category'):

                # deal with questions which have all responses in one columns
                dftmp = pd.concat([self.df[col].replace({self.skipped_code, None})
                                  .value_counts()
                                  .to_frame()
                                  .rename({col: 'count'}, axis=1),
                                   self.df[col].replace({self.skipped_code, None})
                                  .value_counts(normalize=True)
                                  .to_frame()
                                  .rename({col: 'freq'}, axis=1) * 100],
                                  axis=1)

            elif all(self.md.loc[self.md['fldCode'] == col, 'dType'] == 'bool'):

                # deal with questions which have responses boolean responses in several columns
                dftmp = pd.concat([self.df[col].fillna(False).replace({self.skipped_code, None})
                                  .value_counts()
                                  .to_frame()
                                  .rename({col: 'count'}, axis=1),
                                   self.df[col].fillna(False).replace({self.skipped_code, None})
                                  .value_counts(normalize=True)
                                  .to_frame()
                                  .rename({col: 'freq'}, axis=1) * 100],
                                  axis=1)


            elif all(self.md.loc[self.md['fldCode'] == col, 'dType'] == 'str'):

                dftmp = self.df[col].replace({self.skipped_code, None}) \
                    .describe().filter(['count']) \
                    .to_frame().transpose()

            try:
                dftmp['fldCode'] = col
            except:
                pass

            res = pd.concat([res, dftmp], axis=0, ignore_index=False)

        mres = self._get_missing_stats(self.df[sum_columns])

        res.reset_index(inplace=True)
        res.rename({'index': 'optVal'}, axis=1, inplace=True)
        res.loc[res['optVal'] == res['fldCode'], 'optVal'] = None
        res.set_index(['fldCode'], inplace=True)

        res = res.merge(mres, left_index=True, right_index=True)

        # TODO: This should be included where the field is not strictly categorical (str fields for example)

        # ### get n frequent items of categorical columns
        # n = 32
        # freq_items = []
        # for col in sum_columns:
        #     items = self.df[col].value_counts()[:n].index.tolist()
        #     count = self.df[col].value_counts()[:n].tolist()
        #     freq_dict = dict(zip(items, count))
        #     freq_dict = {k: v for k, v in sorted(freq_dict.items(), key=lambda item: item[1],reverse=True)}
        #     freq_items.append(freq_dict)
        #
        # res.loc["frequent_items"] = freq_items
        #
        # res = res.transpose()
        # # print(res)

        return res

    def report_all_columns(self, nested=False):
        """Returns a description of all columns of the dataframe"""

        # TODO: DateTime reporting taken out temporarily, will add this feature again soon

        rep = pd.concat([self.report_numeric_columns(),
                         self.report_categorical_columns()],
                        ignore_index=False, axis=0)

        print(rep)

        if self.infer_types:
            metadata = Metadata.metadata(self.df)

            # TODO: this bit can be replaced with the report formatter
            rep = rep.reset_index().merge(metadata.get_all_column_types(), how='inner', right_on="column",
                                          left_on='index').drop(
                columns={"25%", "50%", "75%", "index"}, errors='ignore')
        else:
            rep.index.name = 'fldCode'
            rep.reset_index(inplace=True)
            rep = self._format_report(rep, nested=nested)

        return rep

    def _format_report(self, rep, nested):

        def report_nester(_rep):
            # a function to produce nested data structure necessary for presenting in DIGIT
            _s = pd.Series()

            # TODO: could probably not include the common fields in the aggregation function
            # set common fields
            # print(_rep['index'])
            _s['index'] = _rep['index'].iloc[0]
            _s['frmCode'] = _rep['frmCode'].unique()[0]
            _s['fldCode'] = _rep.name
            _s['dType'] = _rep['dType'].unique()[0]
            _s['fldTitle'] = _rep['fldTitle'].unique()[0]
            _s['count'] = _rep['count'].sum()
            _s['missings_count'] = _rep['missings_count'].mean()
            _s['missings%'] = _rep['missings%'].mean()
            _s['skipped_count'] = _rep['skipped_count'].mean()

            # set nested fields depending on whether this reported field has more than one result line or not
            if _s['dType'] == 'category':
                cols = ['optVal', 'optText', 'count', 'freq']
                _s['opt'] = _rep[cols].to_dict(orient='records')
            elif _s['dType'] == 'numeric':
                cols = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
                _s['opt'] = _rep[cols].to_dict(orient='records')
            else:
                _s['opt'] = []

            return _s.to_frame().transpose()

        self.md.index.name = 'index'
        md = self.md.reset_index()[['index', 'fldCode', 'fldTitle', 'optVal', 'optText', 'dType', 'frmCode']]

        # rep = rep.merge(self.md[['fldCode', 'fldTitle', 'optVal', 'optText', 'dType', 'frmCode']],
        #                 how='inner',
        #                 left_on=['fldCode', 'optVal'],
        #                 right_on=['fldCode', 'optVal'])

        rep = rep.merge(md,
                        how='inner',
                        left_on=['fldCode', 'optVal'],
                        right_on=['fldCode', 'optVal'])

        rep = rep.fillna(self.nanfill)

        if nested:
            rep = rep.groupby(by='fldCode').apply(report_nester)
            colOrder = ['index', 'frmCode', 'fldCode', 'fldTitle', 'dType', 'count',
                        'missings_count', 'missings%', 'skipped_count', 'opt']
        else:
            colOrder = ['index', 'frmCode', 'fldCode', 'fldTitle', 'dType', 'optText', 'optVal', 'count',
                        'missings_count', 'missings%', 'skipped_count',
                        'freq', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']

        # order = self.md.reset_index()[['index', 'frmCode', 'fldCode', 'optVal']]
        # rep = order.merge(rep, on=['frmCode', 'fldCode', 'optVal'])

        return rep[colOrder]

    def _get_missing_stats(self, _df, dType=None):

        def count_missing(_s, skipped_code):
            # This function counts the number and frequency of missing values taking into account skipped values

            n_total = _s.shape[0]
            counts = _s.value_counts(dropna=False)

            # Try and see if there are skipped values. Return 0 on exception
            try:
                n_skipped = counts[(counts.index == skipped_code) |
                                   (counts.index == str(skipped_code))].values[0]
            except IndexError:
                n_skipped = 0

            # Try and see if there are missing values. Return 0 on exception
            try:
                # return missing counts and missing frequency
                return (counts[counts.index.isnull()].values[0],
                        counts[counts.index.isnull()].values[0] * 100 / (n_total - n_skipped),
                        n_skipped)
            except IndexError:
                return (0, 0, n_skipped)

        report_missing = _df.apply(count_missing, skipped_code=self.skipped_code).transpose()
        report_missing.columns = ['missings_count', 'missings%', 'skipped_count']

        return report_missing

    def init_set_missing_values(self, missing_values=[]):

        # TODO: Implement this functionality in __init__ so missing and skipped codes can be set by user
        # TODO: Replacement of missing and skipped values should happen when data is parsed

        """Set any kind of data which we are considering as null"""
        self.df = self.df.replace({None: np.nan, "NaN": np.nan, "none": np.nan, "na": np.nan})
        for val in missing_values:
            self.df = self.df.replace({val: np.nan})

    def get_missing_values_info(self):
        if not self.df.isnull().values.any():
            return "This dataFrame does't have any missing values!"
        else:
            total = self.df.isnull().sum().sum()
            columns_containing_missing = self.df.columns[self.df.isnull().any()].tolist()
            return f"This DataFrame has {total} missing values!" + \
                   f"\n The following columns contain missing values: {columns_containing_missing} "


