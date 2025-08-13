import numpy as np
import pandas as pd
import collections
import itertools
import json
# from os import path
# import sys
# sys.path.append(path.abspath('../sumit-report'))
from rabitpy.preprocessing import report


def check_dataframe_validity(df, columns=[]):
    if not isinstance(df, pd.DataFrame):
        return "The input variable is not a DataFrame!"
    if df.empty:
        return "The input DataFrame is empty!"
    if not set(columns).issubset(df.columns):
        return "One or more specified column names are not in the DataFrame column list!"
    return "success!"


class metadata:
    """Common base class for all metadata"""
    objCount = 0

    def __init__(self, df):
        self.ReportType = ""
        self.df = df
        metadata.objCount += 1
        validity = check_dataframe_validity(df)
        if validity != "success!":
            raise Exception(validity)

    def type(self, ImputationType):
        pass

    def find_int_columns(self):
        """Finds all the Integer columns"""

        # TODO: is this validuity check necessary here. dataframe is validated on initialization
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        numeric_cols = self.df._get_numeric_data().columns
        int_cols = []
        # converts all non numerics to nan
        self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        for numeric_col in numeric_cols:
            tmp = self.df[numeric_col]
            tmp.dropna(inplace=True)
            # print(np.array_equal(tmp, tmp.astype(int))) ***Same result as line below
            if tmp.astype(float).apply(float.is_integer).all():
                int_cols.append(numeric_col)

        return int_cols

    def find_float_columns(self):
        """Finds all the Floating point columns"""
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        numeric_cols = self.df._get_numeric_data().columns
        float_cols = []
        # converts all non numerics to nan
        self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        for numeric_col in numeric_cols:
            tmp = self.df[numeric_col]
            tmp.dropna(inplace=True)
            if not tmp.astype(float).apply(float.is_integer).all():
                float_cols.append(numeric_col)

        return float_cols

    def find_datetime_columns(self):
        """Finds all the datetime columns"""
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        object_columns = self.df.select_dtypes(include=['object', np.datetime64]).columns
        datetime_columns = []
        for obj in object_columns:
            tmps = self.df[obj].dropna()
            try:
                pd.to_datetime(tmps.to_frame()[obj])
                datetime_columns.append(obj)
            except:
                pass
        return datetime_columns

    def find_object_columns(self):
        """Finds all the categorical columns"""
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        object_columns = list(self.df.select_dtypes(include=['object']).columns)
        datetime_columns = self.find_datetime_columns()
        boolean_columns = self.find_boolean_columns()
        categorical_columns = self.find_categorical_columns()
        return list(set(object_columns) - set(datetime_columns) - set(boolean_columns) - set(categorical_columns))

    def find_boolean_columns(self):
        """Finds all the boolean columns"""
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        object_columns = self.df.select_dtypes(include=['object', 'bool']).columns
        boolean_columns = []
        for obj in object_columns:
            tmp = self.df[obj].dropna()
            if len(tmp.unique()) <= 2:
                boolean_columns.append(obj)

        return boolean_columns

    def find_categorical_columns(self):
        """Finds all the categorical columns"""
        validity = check_dataframe_validity(self.df)
        if validity != "success!":
            raise Exception(validity)

        object_columns = self.df.select_dtypes(include=['object']).columns
        categorical_columns = []
        for obj in object_columns:
            tmps = self.df[obj].dropna()
            try:
                pd.Categorical(tmps.to_frame()[obj])
                categorical_columns.append(obj)
            except:
                pass

        datetime_columns = self.find_datetime_columns()
        boolean_columns = self.find_boolean_columns()
        return list(set(object_columns) - set(datetime_columns) - set(boolean_columns))
        return categorical_columns

    def get_column_names(self):
        return list(self.df.columns.values)

    def get_all_column_types(self):
        """get all column types as dictionary"""
        # Create an empty list of dictionaries with length 5
        types = [{} for _ in range(6)]
        t1 = self.find_int_columns()
        t2 = self.find_float_columns()
        t3 = self.find_object_columns()
        t4 = self.find_boolean_columns()
        t5 = self.find_datetime_columns()
        t6 = self.find_categorical_columns()
        types[0] = dict(zip(t1, itertools.repeat("int64")))
        types[1] = dict(zip(t2, itertools.repeat("float64")))
        types[2] = dict(zip(t3, itertools.repeat("Object")))
        types[3] = dict(zip(t4, itertools.repeat("bool")))
        types[4] = dict(zip(t5, itertools.repeat("datetime64")))
        types[5] = dict(zip(t6, itertools.repeat("category")))
        super_dict = collections.defaultdict(set)
        for d in types:
            for k, v in d.items():
                super_dict[k].add(v)

        def set_default(obj):
            if isinstance(obj, set):
                return list(obj)
            raise TypeError

        super_dict = json.dumps(super_dict, default=set_default)
        res_df = pd.json_normalize(json.loads(super_dict)).transpose().reset_index()

        res_df.rename(columns={res_df.columns[0]: "column", res_df.columns[1]: "dtype"}, inplace=True)
        return res_df

    def produce_metadata(self):
        """Creates Metadata dataframe with initial values"""
        report = Report.report(self.df)
        meta = report.report_all_columns()
        # meta = meta.transpose()
        cat_columns = self.find_categorical_columns()
        bool_columns = self.find_boolean_columns()
        # time_columns = self.find_datetime_columns()
        sum_columns = cat_columns + bool_columns
        n = 32
        freq_items = []
        meta = meta.reset_index()
        meta = meta.set_index("column")
        meta['frequent_items_order'] = None
        for col in sum_columns:
            items = self.df[col].value_counts()[:n].index.tolist()
            order = list(range(1, len(items) + 1))
            freq_dict = dict(zip(items, order))
            meta['frequent_items_order'][col] = freq_dict
            freq_items.append(freq_dict)
        print(meta)
        return meta
