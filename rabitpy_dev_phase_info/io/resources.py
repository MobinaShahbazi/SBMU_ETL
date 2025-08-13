import json
import warnings
import re
from collections import defaultdict
import pandas as pd

from rabitpy_dev_phase_info.utils import timing, find
from .adapters import RabitReaderBaseAdapter, RabitReaderAPIAdapter, RabitReaderJSONFileAdapter, RabitDatabaseAdapter, \
    RabitReaderJSONObjAdapter
from .parser_utils import _get_idx, _flatten_json
from .parsers import _rename_dict, _replace_set, _set_order
from .validity import _check_coding_validity
import requests
from collections import OrderedDict
from urllib.parse import urlparse, parse_qs, urlunsplit
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import functools
import itertools


def json_handler(content):
    if isinstance(content, str):
        try:
            return json.loads(content)
        except:
            return None
    elif isinstance(content, dict) or isinstance(content, list):
        return content


@timing
def _rename_duplicates(md):
    # propagate renamed codes in metadata
    # get all list elements marked as duplicates

    dupcols = ['frmCode', 'fldCode']

    # Drop duplicates of [frmCode, fldCode] first and the see which field codes have duplicates
    dup = md.drop_duplicates(subset=dupcols)
    dup = dup.loc[dup.duplicated(subset='fldCode'), 'fldCode']

    # get boolean array for duplicated values
    ix = md['fldCode'].isin(dup)

    # Assign new names for duplicated values
    md.loc[ix, 'fldCodeR'] = 'frm' + \
                             md.loc[ix, 'frmCode'].astype(str) \
                             + '_' + \
                             md.loc[ix, 'fldCode'].astype(str)

    # Create renaming dictionary in {frmCode: {field1: field1_new_name}} format
    rd = dict.fromkeys(md.loc[ix, 'frmCode'].unique())

    for frm in rd.keys():
        ixx = md['frmCode'] == frm
        rd[frm] = dict(zip(md.loc[ix & ixx, 'fldCode'], md.loc[ix & ixx, 'fldCodeR']))
        rdvc = dict(
            zip(md.loc[ix & ixx, 'fldCode'].apply(lambda x: f"{ {x} }".replace("'", "").replace(' ', '')),
                md.loc[ix & ixx, 'fldCodeR'].apply(lambda x: f"{ {x} }".replace("'", "").replace(' ', ''))
                ))
        md.loc[ix & ixx, 'fldCode'] = md.loc[ix & ixx, 'fldCodeR']

        for k, v in rdvc.items():
            if 'visibleCondition' in list(md.columns):
                md.loc[ixx, 'visibleCondition'] = md.loc[ixx, 'visibleCondition'] \
                    .fillna('').str.replace(k, v, regex=True)
            if 'expression' in list(md.columns):
                md.loc[ixx, 'expression'] = md.loc[ixx, 'expression'].fillna('').str.replace(k, v, regex=True)

    md = md.drop(columns='fldCodeR')

    return md, rd


class RabitBaseResource:

    def __init__(self, source, reader=None, content_path=None, json_path=None, **kwargs):

        """

        Base class for all RABIT resources
        source: Source type corresponding to one of the resource readers in rabit.io.adapters
        **kwargs: Arguments to be passed to RabitReader adapter as specified by 'source'

        """

        self.source = source

        if reader and isinstance(reader.__class__.__bases__[0], RabitReaderBaseAdapter.__class__):
            self.reader = reader
        else:
            self.reader = self.__get_resource_reader(**kwargs)

        self.content_path = content_path
        self.json_path = json_path
        self.raw = None

    @property
    def filters(self):
        return self.reader.filters

    def add_filters(self, filters=None, field=None, condition=None, value=None):

        if isinstance(filters, list) or isinstance(filters, tuple):
            for f in filters:
                if isinstance(f, tuple) or isinstance(f, list):
                    self.__update_filters(*f)
                if isinstance(f, dict):
                    self.__update_filters(**f)
            return None
        elif field and condition and value:
            self.__update_filters(field=field, condition=condition, value=value)
        else:
            warnings.warn('Could not update filter. Check inputs...')

    def reset_filters(self):
        self.reader.reset_filters()

    def __update_filters(self, field, condition, value):
        self.reader.add_filter(field, condition, value)

    def __get_resource_reader(self, **kwargs):

        if self.source == 'api':
            baseurl = kwargs.get('baseurl')
            uri = kwargs.get('uri')
            route = kwargs.get('route')
            verify = kwargs.get('verify', False)
            parameters = kwargs.get('parameters', dict())
            return RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route=route, verify=verify, parameters=parameters)
        elif self.source == 'json-file':
            fp = kwargs.get('fp')
            encoding = kwargs.get('encoding', 'utf-8')
            return RabitReaderJSONFileAdapter(fp=fp, encoding=encoding)
        elif self.source == 'db':
            url = kwargs.get('url')
            query = kwargs.get('query')
            return RabitDatabaseAdapter(url=url, query=query)
        elif self.source == 'json':
            obj = kwargs.get('obj')
            return RabitReaderJSONObjAdapter(obj=obj)
        else:
            raise ValueError(f'Unrecognized source type: {self.source}...')

    @property
    def raw(self):
        return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    def fetch(self, cache=False):

        # Fetch data from target resource
        # A reader should be a class derived from RabitReaderBaseAdapter which have a fetch method
        try:
            fetched = self.reader.fetch()
        except ValueError as e:
            warnings.warn(f'Reader fetch method returned malformed or empty response with error:\n{e.__str__()}')
            return None

        # We need to extract the path where targeted resource content is.
        # The content should ultimately be an array of objects.
        if isinstance(fetched, dict):
            content = find(self.content_path, fetched)
        elif isinstance(fetched, list):
            content = []
            for item in fetched:
                content.append(find(self.content_path, item))

        if not content:
            raise ValueError('No content was fetched from source.')

        del fetched

        # Each RabitResource object may have one JSON path.
        # Some contents are dictionaries where the json path is
        # Some other contents will be in form of records where each has a JSON path
        if self.json_path:
            if isinstance(content, dict):
                content[self.json_path] = json_handler(content.get(self.json_path))
            elif isinstance(content, list):
                for item in content:
                    item[self.json_path] = json_handler(item.get(self.json_path))

        # Cache data in the raw attribute
        if cache:
            self.raw = content

        return content

    def dump(self, fp, indent=4):
        with open(fp, 'w') as f:
            json.dump(self.fetch() if not self.raw else self.raw, f, indent=indent)


class RabitData(RabitBaseResource):

    def __init__(self, source, index_fields, use_fields=None, content_path='content', json_path=None, **kwargs):

        """

        RABIT Data Resource Class
        source: Source type corresponding to one of the resource readers in rabit.io.adapters
        index_fields: A list of dictionary specifying the index fields to be used in data
        use_fields: A list of fields other than parsed json to be included in the parsed output
        json_path: A string specifying the path that contains the response json object
        **kwargs: Arguments to be passed to RabitReader adapter as specified by 'source'

        """

        super().__init__(source=source, json_path=json_path, **kwargs)

        # set all necessary data for this parser
        self.index_fields = index_fields
        self.use_fields = use_fields
        self.content_path = content_path

        self.parser_args = ['index_fields', 'json_path', 'use_fields']
        self.idx = None
        self.raw = None
        self.df = None
        self.dff = None

    def __add__(self, other):
        self.df = pd.concat([self.df, other.df]).reset_index(drop=True)
        return self.df

    @timing
    def parse(self, cache=False):

        d = self.fetch(cache) if not self.raw else self.raw

        # This function returns a dataframe with index columns, user selected fields, and dynamic data counts
        if isinstance(d, dict):
            # find and retrieve the data contained in content_path
            d = find(self.content_path, d)
        elif not isinstance(d, dict) and not isinstance(d, list):
            # TODO: Check if this should throw a ValueError exception
            warnings.warn('Specified data path not found or is empty, check path and try again...')
            return None

        # create list of fields to be extracted and indicate index tags
        self.idx = _get_idx(index_fields=self.index_fields,
                            use_fields=self.use_fields,
                            available_fields=list(d[0].keys()))

        # set all fields to extract form observations
        cols = (list(self.idx.keys()) + [self.json_path]) if self.json_path else list(self.idx.keys())
        dtypes = {item['name']: item.get('dtype', 'str') for item in self.index_fields}

        # construct raw response pandas dataframe
        df = pd.DataFrame().from_records(d, columns=cols, coerce_float=False)

        # construct default value dictionary for indices
        fndct = {k: v.get('default') for k, v in self.idx.items() if v.get('index') and v.get('default') is not None}

        df = df.fillna(fndct)

        # User can set data type for index and use_fields fields
        df = df.astype(dtypes)

        # rename fields according to aliases given in idx
        df = df.rename(columns={k: v['alias'] for k, v in self.idx.items() if v.get('alias')})

        ufs = list(filter(lambda x: not self.idx[x].get('index'), self.idx))
        if ufs:
            df['json'] = df[ufs].to_dict(orient='records')
            df.drop(columns=ufs, inplace=True)
        else:
            df['json'] = [{} for x in range(len(df))]

        if self.json_path:
            try:

                flattened = pd.DataFrame() \
                    .from_records(df[self.json_path]
                                  .apply(lambda x: _flatten_json('', json.loads(x, strict=False), counter={})),
                                  columns=['json_tmp', 'dc'])
            except TypeError:
                flattened = pd.DataFrame() \
                    .from_records(df[self.json_path]
                                  .apply(lambda x: _flatten_json('', x, counter={})),
                                  columns=['json_tmp', 'dc'])

            df.drop(columns=[self.json_path], inplace=True)
            df = pd.concat([df, flattened], axis=1)
            df.apply(lambda x: x['json'].update(x['json_tmp']), axis=1)
            df.drop(columns='json_tmp', inplace=True)
        else:
            df['dc'] = [dict() for x in range(len(df))]

        df['fldCode'] = df['json'].apply(lambda x: list(x.keys()))
        self.df = df
        del df
        self.dff = self.extract_structure()
        self.df = self.df.drop(columns=['fldCode', 'dc'])

    @timing
    def extract_structure(self):

        '''
        THIS FUNCTION WILL CHANGE IN FUTURE, DO NOT TOUCH WITHOUT CONSULTING
        A project consists of phases and forms. This function extract the fields and maximum number of repeated measures
        fields in the response dataframe "df". It was previously being done like:

            dff = self.df.groupby(by=['phase_id', 'frmCode']).agg({'fldCode': 'sum',
                                                      'dc': lambda x: pd.DataFrame().from_records(list(x))
                                                     .max().to_dict()})
            dff['fldCode'] = dff['fldCode'].apply(lambda x: set(x))

            dff.reset_index(inplace=True)

            return dff

        but this was slow. So the currrent approach using itertools and functools was adopted

        return unique keys from a list of dictionaries
        set(itertools.chain.from_iterable(xx)

        return maximum value of key 'k' between all dictionaries in list xx
        functools.reduce(lambda x, y: max(x, y.get(k, 0))
                  if isinstance(x, int) else y.get(k, 0), xx)

        '''

        def get_max_key_value_from_dicts(xx):

            if len(xx) == 1:
                return xx.iloc[0]
            else:
                return {k: functools.reduce(lambda x, y: max(x, y.get(k, 0))
                      if isinstance(x, int) else y.get(k, 0), xx)
                       for k in set(itertools.chain.from_iterable(xx))
                       }

        out = self.df.groupby(by=['phase_id', 'frmCode']) \
            .agg({'fldCode': lambda xx: set(itertools.chain.from_iterable(xx)),
                  'dc': get_max_key_value_from_dicts}).reset_index()

        return out

    def rename_duplicates(self):

        # get rename dictionary for responses regardless of metadata
        _, rndct = _rename_duplicates(self.dff.explode('fldCode'))

        dfgbo = self.df.groupby(by=['frmCode'])
        for name, group in dfgbo:
            group['json'].apply(lambda x: _rename_dict(x, rndct.get(name, {})))
            self.dff.loc[self.dff['frmCode'] == name, 'fldCode'] = self.dff.loc[self.dff['frmCode'] == name, 'fldCode'] \
                .apply(lambda x: _replace_set(x, rndct.get(name, {})))
            self.dff.loc[self.dff['frmCode'] == name, 'dc'] = self.dff.loc[self.dff['frmCode'] == name, 'dc'] \
                .apply(lambda x: _rename_dict(x, rndct.get(name, {})))

    @timing
    def reshape(self, shape='merged', index='pid', apply_phase=True, order=None, filter_array=None, reset_index=False, keep='last'):

        """
        :param order: set the order of forms in the resulting shape
        :param shape:
            - merged: drops duplicated forms and returns all data in a flat arrangement
            - split: returns a list of dataframes eplit on given criteria
        :return:
        """

        # First create the index by which the data will be reshaped
        if not index:
            index = ['pid']
        elif isinstance(index, str):
            index = ['pid'] if index != 'pid' else ['pid', *index]
        elif isinstance(index, list):
            index = list(set(['pid', *index]))
        else:
            raise TypeError('Index can only be a string or a list.')

        if self.df.empty:
            warnings.warn('No data passed. Is data parsed? Skipping...')
            return None

        # Remove unnecessary data
        if filter_array is None:
            filter_array = self.df.index.notna()

        # First filter out unnecessary rows from data
        df = self.df.loc[filter_array]

        if not apply_phase:
            df['phase_id'] = 0

        # Order values bases on order
        if order:
            df = df.sort_values(by=order)

        # Keep or drop duplicates
        if keep in ['last', 'first']:
            df = df.drop_duplicates(subset=index, keep=keep)

        if shape == 'split-forms':
            gbo = df.groupby(by=['frmCode'])
            out = {}
        else:
            gbo = [df]

        for name, obj in gbo:
            # Add prefix to fields as appropriate
            obj = self.add_prefix_to_fields(df=obj)

            # Concatenate response dictionaries
            obj = obj.groupby(by=index).apply(lambda x: self.__concat_response_dicts(x))
            out[name] = obj

        return gbo

    @timing
    def add_prefix_to_fields(self, df=None, has_duplicates=False, rename_duplicates=False):

        if isinstance(df, pd.DataFrame):
            inplace = False
        else:
            inplace = True
            df = self.df.copy()

        # Build prefix automatically
        is_mono_phased = df['phase_id'].nunique() == 1

        if any(df.duplicated(subset=['pid', 'phase_id', 'frmCode'])):
            has_repeated_measure = True
        else:
            has_repeated_measure = False

        if not is_mono_phased and has_repeated_measure:
            raise NotImplementedError('RABIT data has repeated measures')
        elif not is_mono_phased and not has_repeated_measure:
            df['prefix'] = df.apply(lambda x: f"p{x['phase_id']}.f{x['frmCode']}", axis=1)
        elif is_mono_phased and not has_repeated_measure:
            df['prefix'] = df.apply(lambda x: f"p{x['phase_id']}.f{x['frmCode']}", axis=1)
        elif is_mono_phased and has_repeated_measure:
            df['prefix'] = df.apply(lambda x: f"p{x['phase_id']}.f{x['frmCode']}", axis=1)


        df['json'] = df.apply(lambda x: {f"{x['prefix']}.{k}": v for k, v in x['json'].items()}, axis=1)
        df.drop(columns='prefix', inplace=True)

        if inplace:
            self.df = df
        else:
            return df

    def __concat_response_dicts(self, df):

        out = {}
        for item in df['json'].to_list():
            out.update(item)

        return out

    # @timing
    # def __concat_response_duplicated_merge(self, out):
    #
    #     # Maybe add 'frmCode' to sort values
    #     out['prefix'] = out.apply(lambda x: f"p{x['phase_id']}_f{x['frmCode']}", axis=1)
    #     out['json'] = out.apply(lambda x: {f"{x['prefix']}_{k}": v for k, v in x['json'].items()}, axis=1)
    #     out.drop(columns='prefix', inplace=True)
    #     return out


class RabitMetadata(RabitBaseResource):

    def __init__(self, source, reader=None, fid_path='id', json_path='json', include_html=False,
                 frm_name_path='surveyName', frm_desc_path='surveyDescription', base_info_baseurl=None, **kwargs):

        """

        RABIT Data Resource Class
        source: Source type corresponding to one of the resource readers in rabit.io.adapters
        fid_path: Path to form ID
        json_path: Path to json object containing form data
        include_html: Include html objects in metadata output
        rename_duplicates: Rename all non-unique fields
        frm_name_path: Path to form name
        frm_desc_path: Path to form description
        **kwargs: Arguments to be passed to RabitReader adapter as specified by 'source'

        """

        # TODO: Figure out metadata generation from sources which are not RABIT! Like SPSS files and then potentially convert to survey JS forms
        # TODO: When validating, add extra warning for cases where numeric ranges do not have appropriate validation configurations

        super().__init__(source=source, reader=reader, json_path=json_path, **kwargs)

        # set all necessary data for this parser
        self.parser_args = ['fid_path', 'json_path', 'include_html', 'rename_duplicates',
                            'frm_name_path', 'frm_desc_path']
        self._fid_path = fid_path
        self._frm_name_path = frm_name_path
        self._frm_desc_path = frm_desc_path
        self.include_html = include_html
        self.md = None
        self.mdn = None
        self.base_info_baseurl = base_info_baseurl
        self.base_info_data = kwargs.get('base_info_data', None)
        self.comp_md = kwargs.get('comp_md', None)
        self.comp_md_order = kwargs.get('comp_md_order', 'first')
        self._forms = None
        self._has_duplicate_codes = None

    def __add__(self, other):
        self.md = pd.concat([self.md, other.md]).reset_index(drop=True)
        self.base_info_data = pd.concat([self.base_info_data, other.base_info_data]).reset_index(drop=True)
        self.mdn = pd.concat([self.mdn, other.mdn]).reset_index(drop=True)

    @property
    def fid_path(self):
        return self.get_path_property('_fid_path')

    @property
    def frm_name_path(self):
        return self.get_path_property('_frm_name_path')

    @property
    def frm_desc_path(self):
        return self.get_path_property('_frm_desc_path')

    @property
    def forms(self):
        return self._forms

    @property
    def duplicate_codes(self):
        return self.mdn.duplicated(subset='fldCode')

    def fields(self, fid=None, orient='title'):

        if fid is None:
            fid = list(self._forms.keys())

        if isinstance(fid, list):
            fid = list(map(lambda x: str(x), fid))
        else:
            fid = [str(fid)]

        if isinstance(self.mdn, pd.DataFrame):
            if orient == 'title':
                return self.mdn.loc[self.mdn['frmCode'].astype(str).isin(fid), 'fldTitle'].to_list()
            elif orient == 'code':
                return self.mdn.loc[self.mdn['frmCode'].astype(str).isin(fid), 'fldCode'].to_list()
            elif orient == 'grouped':
                return self.mdn.loc[self.mdn['frmCode'].astype(str).isin(fid), ['frmCode', 'fldCode', 'fldTitle']]\
                    .groupby(by='frmCode')\
                    .apply(lambda x: x.set_index('fldCode')['fldTitle'].to_dict()).to_dict()
            elif orient == 'dict':
                return self.mdn.loc[self.mdn['frmCode'].astype(str).isin(fid), ['frmCode', 'fldCode', 'fldTitle']] \
                    .set_index('fldCode')['fldTitle'].to_dict()
        else:
            return []

    def get_path_property(self, prop):
        x = getattr(self, prop)
        if isinstance(x, dict):
            x['default'] = x.get('default')
            return x
        elif isinstance(x, str):
            return {'name': x, 'default': None}

    def parse(self, cache=False, rename_duplicates=True, sort_values=True, check_coding_validity=True,
              append_comp=True, nest=True):

        # This is the main function for parsing metadata
        if not self.json_path:
            return None

        # Fetch data from reader if not previously cached and parse
        # TODO: Should include some form of forced update where raw data is available but underlying data has changed
        d = self.fetch(cache) if not self.raw else self.raw
        metadata = self.__parse(d)

        if metadata is None:
            return None, None

        md = pd.DataFrame().from_records(metadata)
        md = md.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Parse complimentary metadata
        if isinstance(self.comp_md, RabitMetadata):
            if append_comp:
                # No need to check validity and sort the dataframe if going to be appended to md
                self.comp_md.parse(sort_values=False, check_coding_validity=True, nest=False, rename_duplicates=False)

                if self.comp_md_order == 'first':
                    md = pd.concat([self.comp_md.md, md])
                elif self.comp_md_order == 'last':
                    md = pd.concat([md, self.comp_md.md])
                else:
                    raise ValueError(f'Unknown option {self.comp_md_order} for metadata orders...')
            else:
                # Sort values and check validity if comp_metadata is not to be appended to md
                self.comp_md.parse()

        # Check general coding integrity
        if check_coding_validity:
            md = _check_coding_validity(md=md, nested=False)

        # Set field orders
        if sort_values:
            md = _set_order(md, {'frmCode': 'frmOrder', 'fldCode': 'fldOrder', 'fldParentCode': 'fldParentOrder'})

        self.md = md

        if nest:
            self.mdn = self.nest()

        self._forms = self.md.drop_duplicates('frmCode').set_index('frmCode')['frmName'].to_dict()

        if rename_duplicates:
            self.rename_duplicates()

    def __parse(self, d):

        metadata = []

        if not isinstance(d, list):
            d = [d]

        for qnr in d:

            fid = qnr.get(self.fid_path['name'], self.fid_path['default'])
            frm_name = qnr.get(self.frm_name_path['name'], self.frm_name_path['default']) if self.frm_name_path else ''
            frm_desc = qnr.get(self.frm_desc_path['name'], self.frm_desc_path['default']) if self.frm_desc_path else ''

            # check and return exception if metadata json does not contain 'json' key
            qnrjson = qnr.get(self.json_path)

            if not qnrjson:
                warnings.warn(f'Form ID: {fid}: NULL JSON detected. Skipping')
                continue

            try:
                if isinstance(qnrjson, str):
                    qnrjson = json.loads(qnrjson, strict=False)
            except KeyError as e:
                raise KeyError(f'Form ID: {fid}: Invalid JSON...')

            # check if there are 'pages' key, if not, raise error
            pages = qnrjson.get('pages', None)
            if not pages:
                raise KeyError(f'Form ID: {fid}: pages element missing from JSON.')

            # for each page in pages go through each element and parse metadata
            for page in pages:
                # If page does not contain elements key, create empty list which means we continue on this loop
                for element in page.get('elements', []):
                    try:
                        metadata += self.__set_properties(_qnr=qnr,
                                                          _element=element,
                                                          _choices=self.__extract_choices(_el=element),
                                                          _fid=fid,
                                                          _frm_name=frm_name,
                                                          _frm_desc=frm_desc,
                                                          baseurl=self.base_info_baseurl,
                                                          _include_html=self.include_html)

                    # Add errors to output dict where there are exceptions
                    except Exception as e:

                        print(f'error in processing {fid} {element.get("name")}')

                        metadata.append({'fldCode': element.get('name'),
                                         'fldTitle': element.get('title'),
                                         'frmCode': fid,
                                         'frmName': frm_name,
                                         'frmDesc': frm_desc,
                                         'frmOrder': qnr.get('sortOrder'),
                                         'error': e})
                        raise
        return metadata

    def __set_properties(self, _qnr=None, _element=None, _choices=None, _fid=None, _frm_name=None, _frm_desc=None,
                         baseurl=None, _include_html=False):

        if _element['type'] == 'html' and not _include_html:
            return []

        # This function parses single elements in a questionnaire
        properties = []

        this = OrderedDict()
        updater = lambda x: properties.append(x) if isinstance(this, OrderedDict) else properties + this

        # Multiple text
        if _element.get('type') == 'multipletext':

            for item in _element.get('items'):
                # copy item into a recursive element
                rec_element = item

                # flatten parent and child element codes
                rec_element.update({'name': '{}_{}'.format(_element.get('name', ''), item.get('name')),
                                    'type': 'text',
                                    'visibleIf': _element.get('visibleIf'),
                                    'fldParentCode': _element.get('name', ''),
                                    'fldParentTitle': self.__translation_handler(s=_element, target='title', alt='')})

                # send the element back to the function and update properties
                this = self.__set_properties(_qnr=_qnr, _element=rec_element, baseurl=baseurl,
                                             _fid=_fid, _frm_name=_frm_name, _frm_desc=_frm_desc)

                if isinstance(this, dict):
                    this['elementType'] = f'multipletext - {this["elementType"]}'
                elif isinstance(this, list):
                    for item in this:
                        item['elementType'] = f'multipletext - {item["elementType"]}'

                properties = updater(this.copy())

            return properties

        # Matrix
        if _element.get('type') == 'matrix':

            fldAx = _element.get('rows')
            optAx = _element.get('columns')

            for fld in fldAx:

                fldtmp = {}

                if isinstance(fld, dict):
                    fldtmp['name'] = '{}_{}'.format(_element.get('name'), fld.get('value'))
                    fldtmp['title'] = '{} - {}'.format(
                        self.__translation_handler(s=_element, target='title', alt=_element.get('name')),
                        self.__translation_handler(s=fld, target='text', alt=fld.get('name')))

                elif isinstance(fld, str):
                    fldtmp['name'] = '{}_{}'.format(_element.get('name'), fld)
                    fldtmp['title'] = fld

                else:
                    raise TypeError(f'Matrix row type is {type(fld)} and not dict or string...')

                fldtmp.update({'choices': optAx,
                               'type': 'radiogroup',
                               'visibleIf': _element.get('visibleIf'),
                               'fldParentCode': _element.get('name'),
                               'fldParentTitle': self.__translation_handler(s=_element, target='title',
                                                                      alt=_element.get('name'))
                               })

                # send the element back to the function and update properties
                this = self.__set_properties(_qnr=_qnr,
                                             _element=fldtmp,
                                             _choices=_choices,
                                             _fid=_fid,
                                             _frm_name=_frm_name,
                                             _frm_desc=_frm_desc,
                                             baseurl=baseurl)

                if isinstance(this, dict):
                    this['elementType'] = f'matrix - {this["elementType"]}'
                elif isinstance(this, list):
                    for item in this:
                        item['elementType'] = f'matrix - {item["elementType"]}'

                properties = updater(this.copy())
            return properties

        # Dropdown Matrix
        if _element.get('type') == 'matrixdropdown':

            fldAx = _element.get('rows')
            optAx = _element.get('columns')

            for row in fldAx:

                ttl = self.__translation_handler(s=_element, target='title', alt=_element.get('name'))
                if isinstance(row, dict):
                    parentFldTitle = f"{ttl} - {row.get('text', row.get('value'))}"
                    parentFldCode = f"{_element.get('name')}_{row.get('value')}"
                elif isinstance(row, str):
                    parentFldTitle = f"{ttl} - {row}"
                    parentFldCode = f"{_element.get('name')}_{row}"
                else:
                    raise TypeError(f'Matrix row type is {type(row)} and not dict or string...')

                fld = {}

                for col in optAx:
                    fld['type'] = col.get('cellType', 'dropdown')
                    # TODO: Check visibleIf condition for column and rows and implement if necessary
                    # fld['visibleIf'] = _element.get('visibleIf', None)

                    ttl = self.__translation_handler(s=col, target='title', alt=col.get('name'))
                    fld['title'] = f"{parentFldTitle} - {ttl}"
                    fld['name'] = f"{parentFldCode}_{col.get('name')}"
                    fld['fldParentCode'] = parentFldCode
                    fld['fldParentTitle'] = parentFldTitle

                    # send the element back to the function and update properties
                    this = self.__set_properties(_qnr=_qnr,
                                                 _element=fld,
                                                 _choices=self.__extract_choices(_el=col),
                                                 _fid=_fid,
                                                 _frm_name=_frm_name,
                                                 _frm_desc=_frm_desc,
                                                 baseurl=baseurl)

                    if isinstance(this, dict):
                        this['elementType'] = f'matrixdropdown - {this["elementType"]}'
                    elif isinstance(this, list):
                        for item in this:
                            item['elementType'] = f'matrixdropdown - {item["elementType"]}'

                    properties = updater(this.copy())

            return properties

        # Panel
        if _element.get('type') == 'panel':
            for el in _element.get('elements', []):

                el['name'] = '{}'.format(el.get('name'))
                el['fldParentCode'] = el.get('name')
                el['fldParentTitle'] = self.__translation_handler(s=el, target='title', alt=el.get('name'))

                this = self.__set_properties(_qnr=_qnr,
                                             _element=el,
                                             _fid=_fid,
                                             _choices=self.__extract_choices(_el=el),
                                             _frm_name=_frm_name,
                                             _frm_desc=_frm_desc,
                                             baseurl=baseurl)

                if isinstance(this, dict):
                    this['elementType'] = f'panel - {this["elementType"]}'
                elif isinstance(this, list):
                    for item in this:
                        item['elementType'] = f'panel - {item["elementType"]}'

                properties = updater(this.copy())
            return properties

        # Dynamic Matrix
        if _element.get('type') == 'matrixdynamic':
            warnings.warn('Dynamic matrix parsing is currently experimental...')
            optAx = _element.get('columns')

            parentFldTitle = f"{self.__translation_handler(s=_element, target='title', alt=_element.get('name'))}"
            parentFldCode = f"{_element.get('name')}_r1"
            fld = {}

            for col in optAx:
                fld['type'] = col.get('cellType', 'dropdown')

                # TODO: Check visibleIf condition for column and rows and implement if necessary
                # fld['visibleIf'] = _element.get('visibleIf', None)

                fld['title'] = f"{parentFldTitle} - {self.__translation_handler(s=col, target='title', alt=col.get('name'))}"
                fld['name'] = f"{parentFldCode}_{col.get('name')}"

                # send the element back to the function and update properties
                this = self.__set_properties(_qnr=_qnr,
                                             _element=fld,
                                             _choices=self.__extract_choices(_el=col),
                                             _fid=_fid,
                                             _frm_name=_frm_name,
                                             _frm_desc=_frm_desc,
                                             baseurl=baseurl)

                if isinstance(this, dict):
                    this['elementType'] = f'matrixdynamic - {this["elementType"]}'
                elif isinstance(this, list):
                    for item in this:
                        item['elementType'] = f'matrixdynamic - {item["elementType"]}'

                properties = updater(this.copy())

            return properties

        # Dynamic panel
        if _element.get('type') == 'paneldynamic':
            warnings.warn('Dynamic panel parsing is currently experimental...')
            for el in _element.get('templateElements', []):

                el['name'] = f"{_element.get('name')}_r1_{el.get('name')}"

                if el['type'] not in ['matrix', 'matrixdropdown', 'matrixdynamic']:
                    this = self.__set_properties(_qnr=_qnr,
                                                 _element=el,
                                                 _fid=_fid,
                                                 _choices=self.__extract_choices(_el=el),
                                                 _frm_name=_frm_name,
                                                 _frm_desc=_frm_desc,
                                                 baseurl=baseurl)
                else:
                    this = self.__set_properties(_qnr=_qnr,
                                                 _element=el,
                                                 _fid=_fid,
                                                 _choices=None,
                                                 _frm_name=_frm_name,
                                                 _frm_desc=_frm_desc,
                                                 baseurl=baseurl)

                if isinstance(this, dict):
                    this['elementType'] = f'paneldynamic - {this["elementType"]}'
                elif isinstance(this, list):
                    for item in this:
                        item['elementType'] = f'paneldynamic - {item["elementType"]}'

                properties = updater(this.copy())
            return properties

        # set common properties, the type property is set here once and if necessary it will be reset in later steps
        this['frmCode'] = _fid
        this['frmName'] = _frm_name
        this['frmDesc'] = _frm_desc
        this['fldCode'] = _element.get('name')
        this['fldTitle'] = self.__translation_handler(_element, 'title', _element.get('name'))
        this['fldParentCode'] = _element.get('fldParentCode', this['fldCode'])
        this['fldParentTitle'] = _element.get('fldParentTitle', this['fldTitle'])
        this['elementType'] = _element.get('type')
        this['visibleCondition'] = _element.get('visibleIf', '')
        this['expression'] = _element.get('expression', '')
        this['validators'] = _element.get('validators')
        this['optVal'] = None
        this['optText'] = None
        this['opt'] = None

        # set data type for field
        if this.get('elementType') == 'text':
            if _element.get('inputType', '') == 'number':
                this.update({'dType': 'numeric'})
            elif _element.get('inputType', '') == 'date':
                this.update({'dType': 'datetime'})
            elif _element.get('inputType', '') == 'date-jalali':
                this.update({'dType': 'jalalidate'})
            else:
                this.update({'dType': 'str'})

        if this.get('elementType') == 'expression':
            this.update({'dType': 'numeric'})

        elif this.get('elementType') in ['comment', 'html', 'file']:
            this.update({'dType': 'str'})

        elif this.get('elementType') in ['radiogroup', 'dropdown', 'rating', 'boolean']:
            this.update({'dType': 'category'})

        elif this.get('elementType') in ['checkbox', 'tagbox']:
            this.update(({'dType': 'bool'}))

        # deal with input types which have choice element
        if _choices is not None and this.get('elementType') not in ['matrixdropdown', 'matrixdynamic']:
            parentFldTitle = this.get('fldTitle')
            parentFldCode = this.get('fldCode')
            opt = []

            for _choice in _choices:
                # set value and label of choice
                if isinstance(_choice, dict):
                    chValue = str(_choice.get('value')).strip()
                    chText = self.__translation_handler(_choice, 'text', _choice.get('value'))
                else:
                    chValue = str(_choice).strip()
                    chText = str(_choice).strip()

                # update name and field code of checkbox type quesitons, update and continue
                if this.get('elementType') == 'checkbox':
                    this.update({'fldTitle': '{} - {}'.format(parentFldTitle, chText),
                                 'fldCode': '{}_{}'.format(parentFldCode, chValue)})

                    this.update({'optVal': chValue, 'optText': chText})
                    updater(this.copy())
                    continue

                this.update({'optVal': chValue, 'optText': chText})
                updater(this.copy())

            if opt:
                updater(this.copy())

        else:
            updater(this.copy())

        # Add 'other' option if it is active
        if _element.get('hasOther'):
            this.pop('optVal')
            this.pop('optText')
            this.update({'dType': 'str', 'elementType': 'text', 'fldCode': '{}_{}'.format(_element['name'], 'comment')})
            this.update({'fldTitle': '{} - {}'.format(parentFldTitle, this.get('otherText', 'other'))})
            updater(this.copy())

        return properties

    def __extract_choices(self, _el, limit=20):

        if _el.get('type') == 'boolean':
            return [{'value': True, 'text': _el.get('labelTrue', 'Yes')},
                    {'value': False, 'text': _el.get('labelFalse', 'No')}]

        if _el.get('choices', _el.get('rateValues')):
            return _el.get('choices', _el.get('rateValues'))

        if not isinstance(self.base_info_data, pd.DataFrame):
            self.base_info_data = pd.DataFrame(columns=['varCode', 'value', 'text'])

        if _el.get('choicesByUrl') or _el.get('choicesByDynamicUrl'):

            # We need to find what code we are looking at in the baseinfo object
            parsed_url = urlparse(_el.get('choicesByUrl', _el.get('choicesByDynamicUrl')).get('url'))
            query_params = parse_qs(parsed_url.query)
            code = query_params['code'][0]

            if code in self.base_info_data['varCode'].unique():
                tmp = self.base_info_data.loc[self.base_info_data['varCode'] == code]
                pass
            else:

                # This bit anticipates the baseinfo response to be a list of objects each like
                # [{'code': 'var1_code', 'title': 'var1_title'}, {'code': 'var2_code', 'title': 'var2_title'}]
                content_path = _el.get('path', None)
                value_name = _el.get('valueName', 'code')
                title_name = _el.get('titleName', 'title')

                retry_strategy = Retry(connect=1)
                adapter = HTTPAdapter(max_retries=retry_strategy)
                session = requests.Session()
                session.mount(prefix='https://', adapter=adapter)
                session.mount(prefix='http://', adapter=adapter)

                for scheme in ['https', 'http']:

                    if parsed_url.hostname:
                        url_scheme = parsed_url.scheme if parsed_url.scheme else scheme
                        baseurl = parsed_url.hostname
                    else:
                        parsed_baseurl = urlparse(self.base_info_baseurl)
                        url_scheme = parsed_baseurl.scheme if parsed_baseurl.scheme else scheme
                        baseurl = parsed_baseurl.hostname

                    url_path = parsed_url.path.replace('getValuesDynamic', 'getValues')
                    url = urlunsplit((url_scheme, baseurl, url_path, f'code={code}', ''))

                    try:
                        print(f'Extracting choices from {url}')
                        response = session.get(url, verify=False, timeout=2)
                        r = response.json()
                        if isinstance(r, dict) and r.get('content'):
                            r = r.get('content')
                        tmp = pd.DataFrame(r, columns=['varCode', value_name, title_name])\
                            .rename(columns={value_name: 'value', title_name: 'text'})

                        self.base_info_data = pd.concat([self.base_info_data, tmp])
                        self.base_info_data['varCode'] = self.base_info_data['varCode'].fillna(code)

                        break
                    except ConnectionError as e:
                        warnings.warn(f'Failed to extract choices from {url}. Check if address exists.')
                        return []
                    except requests.exceptions.JSONDecodeError:
                        print(f'Error extracting choice data from {url}')

            if limit:
                return tmp[['value', 'text']][:limit].to_dict(orient='records')
            else:
                return tmp[['value', 'text']].to_dict(orient='records')

    def __translation_handler(self, s, target, alt):
        if target in s:
            if isinstance(s[target], dict):
                return s[target].get('fa', s[target].get('default'))
            else:
                return s[target]
        else:
            return alt

    def nest(self):

        if self.md is None:
            warnings.warn('Metadata is empty. Has it been parsed?')
            return None

        if 'phase_id' in self.md.columns:
            cols = ['phase_id', 'frmCode', 'fldCode']
        else:
            cols = ['frmCode', 'fldCode']

        mdn = self.md.drop_duplicates(cols).drop(columns=['opt'])
        mdn.loc[mdn['optVal'].isna(), 'opt'] = None

        opts = self.md.loc[self.md['optVal'].notna(), cols + ['optVal', 'optText']].copy()

        if len(opts) > 0:
            opts['opt'] = opts.apply(lambda x: {'optVal': x['optVal'], 'optText': x['optText']}, axis=1)
            opts = opts.groupby(cols)['opt'].apply(lambda x: x.to_list()).reset_index()
            mdn = mdn.drop(columns=['opt'])
            mdn = opts.merge(mdn, on=cols, how='right')
        else:
            print('here')

        try:
            mdn = mdn.drop(columns=['optVal', 'optText'])
            mdn = _check_coding_validity(md=mdn, nested=True)
        except:
            print('exception!')

        return _set_order(mdn, {'frmCode': 'frmOrder', 'fldCode': 'fldOrder', 'fldParentCode': 'fldParentOrder'})

    def rename_duplicates(self):
        self.md, _ = _rename_duplicates(self.md)
        self.nest()


class RabitProject(RabitBaseResource):

    def __init__(self, source, reader=None, project_id=None, content_path=None, json_path=None):
        super().__init__(source=source, reader=reader, content_path=content_path, json_path=json_path)
        print('initiated')

        self.project_id = project_id
        self.project_type = None
        self.project_name = None
        self.project_structure = None
        self.has_phases = False
        self._phases = None
        self.phase_records = []

    def set_phases(self):

        cols = ['id', 'name', 'level', 'order', 'createdDate', 'modifiedDate', 'parentId', 'surveyIds']
        rndct = {'id': 'phase_id', 'name': 'phase_title', 'surveyIds': 'phase_frmCode',
                 'order': 'phase_order', 'level': 'phase_level', 'parentId': 'phase_parent_id',
                 'createdDate': 'phase_created_date', 'modifiedDate': 'phase_modified_date'}

        phases = pd.DataFrame().from_records(self.phase_records, columns=cols, coerce_float=False)\
            .sort_values(by=['order', 'level']).rename(columns=rndct)

        if not phases.empty:
            phases['phase_alias'] = 'p' + phases['phase_order'].astype(str).apply(lambda x: x.zfill(2))

        self.phases = phases.explode('phase_frmCode')

        return None

    def add_phase_record(self, records):
        if isinstance(records, dict):
            records = [records]
        self.phase_records = [*self.phase_records, *records]
        self.set_phases()

    def update_phase_record(self, records):
        if isinstance(records, dict):
            records = [records]
        for record in records:
            try:
                target_record = list(filter(lambda x: x['id'] == record['id'], self.phase_records))[0]
                forms = target_record['surveyIds'] if 'surveyIds' in record.keys() else None
                target_record.update(record)
                if forms:
                    target_record['surveyIds'] = list(set([*forms, *record['surveyIds']]))
            except KeyError:
                raise KeyError('Phase records must have id')
            except IndexError:
                raise IndexError('Target phase id does not exist in phase definitions')
        self.set_phases()

    def parse(self, cache=False):

        d = self.fetch(cache) if not self.raw else self.raw

        if not d:
            return None

        self.project_name = d.get('projectName', d.get('id'))
        self.project_structure = d.get('structureAlias')
        self.project_type = d.get('projectTypeAlias')

        # Get phase array as a dataframe
        # Select useful columns from phases metadata

        if d.get('phases'):

            self.phase_records = [x for x in d['phases'] if not x.get('deleted', False)]
            if self.phase_records is not []:
                self.has_phases = True
            else:
                self.has_phases = False

        self.set_phases()

        return None


class RabitDataset:

    def __init__(self, data=None, metadata=None, project=None):

        self.data = data
        self.metadata = metadata
        self.project = project
        self.structure = None
        self._df = None
        self._idx = None
        self._md = None
        self._mdn = None
        self._dff = None
        self._phases = None

    def __add__(self, other):

        if not isinstance(other, RabitDataset):
            raise TypeError(f'The other object must be a RabitDataset, not of type {type(other)}')
        else:
            self.data + other.data
            self.metadata + other.metadata


    @property
    def df(self):
        return self.data.df

    @property
    def dff(self):

        # TODO: This requires considerable updates.

        if self._dff is not None:
            pass

        if self.df is not None and self.md is not None:
            self._dff = self.md.groupby(by=['frmCode']) \
                .agg({'fldCode': lambda x: dict.fromkeys(set(x))}) \
                .reset_index().merge(self.data.dff[['phase_id', 'frmCode', 'dc']], on='frmCode')
        elif self.df is not None:
            self._dff = self.data.dff
            self._dff['fldCode'] = self._dff['fldCode'].apply(lambda x: dict.fromkeys(list(x)))
        else:
            self._dff = None

        return self._dff

    @property
    def idx(self):
        return self.data.idx

    @property
    def md(self):
        return self.metadata.md

    @property
    def mdn(self):
        return self.metadata.mdn

    @property
    def phases(self):
        return self.project.phases

    def load(self, cache=False, load_project=True, load_metadata=True, load_data=True):

        '''
            loads the preconfigured RABIT dataset.
        '''

        # Load project resource if provided
        if self.project and load_project:
            self.project.parse(cache)

        # load metadata resource if provided
        if self.metadata and load_metadata:
            self.metadata.parse(cache, rename_duplicates=False)

        # load data resource if provided
        if self.data and load_data:
            self.data.parse(cache)

        # Each form in RABIT is ultimately part of a phase.
        self.sync_dataset()

        return None

    def sync_dataset(self):

        # When we load a dataset it should either use the given metadata or generate one
        if ~self.md.empty:

            if any(self.df['phase_id'] == 0):
                forms_list = list(self.df.loc[self.df['phase_id'] == 0, 'frmCode'].unique())
                if self.project:
                    self.project.update_phase_record([{'id': 0, 'surveyIds': forms_list}])

            # Create a metadata with phase information and set field orders
            self.metadata.md = self.phases.merge(self.md, left_on='phase_frmCode', right_on='frmCode', how='right') \
                .drop(columns=['phase_frmCode'])

            self.update_dynamic_fields_in_metadata()

            if self.project and self.project.has_phases:
                self.metadata.md['master_code'] = self.metadata.md.apply(
                    lambda x: f"p{x['phase_id']}.f{x['frmCode']}.{x['fldCode']}", axis=1)
                self.metadata.md['master_title'] = self.metadata.md.apply(
                    lambda x: f"{x['phase_alias']} {x['frmName']} {x['fldTitle']}", axis=1)
                # self.metadata.md['frmCode'] = self.metadata.md.apply(lambda x: f"p{x['phase_id']}.f{x['frmCode']}",
                #                                                      axis=1)
                # self.metadata.md['frmName'] = self.metadata.md.apply(lambda x: f"{x['phase_alias']} {x['frmName']}",
                #                                                      axis=1)
                # self.metadata.md['fldCode'] = self.metadata.md['master_code']
                # self.metadata.md['fldTitle'] = self.metadata.md['master_title']
            else:
                self.metadata.md['master_code'] = self.md.apply(lambda x: f"p.f{x['frmCode']}.{x['fldCode']}", axis=1)
                self.metadata.md['master_title'] = self.md.apply(lambda x: f"{x['frmName']} {x['fldTitle']}", axis=1)

                # Any duplicate codes will automatically result in field codes to be of form
                fields = self.mdn.loc[self.mdn.duplicated(subset=['frmCode', 'fldCode'], keep=False), 'fldCode']
                if any(fields):
                    ix = self.metadata.md['fldCode'].isin(fields)
                    self.metadata.md.loc[ix, 'fldCode'] = self.md.loc[ix].apply(lambda x: f"f{x['frmCode']}_{x['fldCode']}", axis=1)

            self.metadata.md = self.metadata.md.sort_values(
                by=['phase_order', 'frmOrder', 'fldParentOrder', 'repeated_measure_number', 'fldOrder'])
            self.metadata.mdn = self.metadata.nest()

    def find_max_between_dicts(self, dicts):
        out = {}
        for d in dicts:
            for k, v in d.items():
                out[k] = v if v > out.get(k, 0) else out[k]
        return out

    def update_dynamic_fields_in_metadata(self, lang='en'):

        # repeated measure title post fix. For adding to repeated measure fields
        rmtpf = {'en': 'r'}

        self.metadata.md['repeated_measure_number'] = 1

        target_fields = self.dff.set_index(['phase_id', 'frmCode'])['dc'].reset_index()
        target_fields = target_fields.loc[target_fields['dc'] != {}]

        # get all dynamic fields in the metadata
        ix = (self.md['elementType'].str.startswith('matrixdynamic')) | \
             (self.md['elementType'].str.startswith('paneldynamic'))
        dynamic_fields = self.md.loc[ix].to_dict(orient='records')

        if not dynamic_fields:
            return None

        new_metadata_rows = []

        # for each record in dynamic fields add them to metadata
        for fld in dynamic_fields:

            ix = (target_fields['frmCode'] == fld['frmCode']) & (target_fields['phase_id'] == fld['phase_id'])

            try:
                fc = target_fields.loc[ix, 'dc'].to_list()[0]
                k = list(filter(lambda x: fld['fldCode'].startswith(f'{x}_r'), fc))
            except:
                continue

            try:
                n = int(fc[k[0]])

                for i in range(2, n + 1):
                    this_fld = fld.copy()
                    this_fld['fldCode'] = re.sub(f'^{k[0]}_r\d+_', f'{k[0]}_r{i}_', fld['fldCode'])
                    this_fld['fldParentCode'] = re.sub(f'^{k[0]}_r\d+_', f'{k[0]}_r{i}_', fld['fldParentCode'])
                    this_fld['fldTitle'] = f'{this_fld["fldTitle"]} - {rmtpf[lang]}{i}'
                    this_fld['fldParentTitle'] = f'{this_fld["fldParentTitle"]} - {rmtpf[lang]}{i}'
                    this_fld['repeated_measure_number'] = i

                    # TODO: Need to preserve field orders here
                    new_metadata_rows.append(this_fld)
                    # self.metadata.md = pd.concat([self.md, pd.Series(this_fld).to_frame().T])

            except IndexError:
                pass
            except ValueError:
                pass

        self.metadata.md = pd.concat([self.md, pd.DataFrame(new_metadata_rows)])
        self.metadata.md.reset_index(drop=True, inplace=True)

        return None

    @timing
    def sync(self):
        d = self.df.merge(self.dff[['frmCode', 'fldCode']], on='frmCode', how='right')
        d['json'] = d.apply(lambda x: {k: x['json'].get(k, None) for k in x['fldCode'].keys()}, axis=1)
        self.data.df = d.drop(columns=['fldCode'])
        del d

    @timing
    def export_data(self, shape=None, remap_fields=False, remap_values=False, **kwargs):

        '''
            shape: shape of the output
            remap_fields: if fields should be exported with titles
            remap_values: if values should be exported as titles
        '''

        if self.df is None:
            return None
        elif shape:
            items = self.reshape(shape=shape, **kwargs)
        else:
            items = self.df

        d_out = {}

        # Align output according to metadata
        if isinstance(self.md, pd.DataFrame) and isinstance(self.phases, pd.DataFrame):

            # create an empty dataframe for the output and align
            # TODO: To align, we create an empty frame resulting in the warning below
            #  The behavior of array concatenation with empty entries is deprecated. In a future version,
            #  this will no longer exclude empty items when determining the result dtype. To retain the old behavior,
            #  exclude the empty entries before the concat operation.
            for name, item in items:

                # Set output columns on the template dataframe
                out_cols = [*self.md.loc[self.md['frmCode'] == name, 'fldCode'].unique()]
                index = pd.MultiIndex.from_tuples(item.set_index(['pid', 'phase_id', 'fillDate']).index, names=['pid', 'phase_id', 'fillDate'])
                df = pd.DataFrame(data=item.json.to_list(), columns=out_cols, index=index).reset_index()

                if shape == 'split-forms':
                    fields_translation = dict(zip(self.md['fldCode'], self.md['fldTitle']))
                elif remap_fields and self.project.has_phases and kwargs.get('apply_phase', False):
                    fields_translation = dict(zip(self.md['master_code'], self.md['master_title']))
                elif remap_fields and self.project.has_phases and not kwargs.get('apply_phase', False):
                    fields_translation = dict(zip(self.md['master_code'], self.md['fldTitle']))
                elif remap_fields and not self.project.has_phases:
                    fields_translation = dict(zip(self.md['master_code'], self.md['fldTitle']))
                elif not self.project.has_phases:
                    fields_translation = dict(zip(self.md['master_code'], self.md['fldCode']))
                else:
                    fields_translation = None

                if remap_values:
                    for form in self.md['frmCode'].unique().tolist():
                        values_map = self.md.loc[(self.md['frmCode'] == form) & (self.md['optVal'].notna())]\
                            .groupby(by=['fldCode'])\
                            .apply(lambda x: dict(zip(x['optVal'], x['optText']))).to_dict()
                        df = df.replace(to_replace=values_map)

                if fields_translation:
                    df = df.rename(columns=fields_translation)

                d_out.update({name: df})
        else:
            d_out = items

        del df

        return d_out

    def reshape(self, **kwargs):

        '''
            shape:
                -merged: returns a single dataframe with all data flattened and merged based on index.
                -split: returns a list of dataframes merged based on split_by and index.
            index: a string or a list of column names by which data should be merged
            split_by: A list of columns to split the DataFrame on
            order: a list or dictionary by which columns should be sorted. See pandas sort_values
            filter_array: a logical list to be sent to pd.DataFrame.loc
            reset_index: Whether to reset the index or not after grouping
            keep: Which duplicated rows to drop based on dataframe sort order
                -last: Picks the last one
                -first: picks the first one
        '''

        if self.project:
            apply_phase = kwargs.get('apply_phase', self.project.has_phases)

        if isinstance(self.md, pd.DataFrame):
            return self.data.reshape(shape=kwargs.get('shape', 'merged'),
                                     index=kwargs.get('index', 'pid'),
                                     apply_phase=apply_phase,
                                     order=kwargs.get('order', ['pid', 'phase_id', 'frmCode', 'fillDate']),
                                     keep=kwargs.get('keep', 'last'),
                                     filter_array=kwargs.get('filter_array', None),
                                     reset_index=kwargs.get('reset_index', False))

        return self.data.reshape(**kwargs)
