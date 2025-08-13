import json
import warnings

from .adapters import RabitReaderAPIAdapter, RabitReaderJSONFileAdapter
from .parsers import _parse_data, _parse_metadata, _generate_metadata, \
    _set_order, _check_coding_validity, _rename_duplicates, _sync_data_metadata


class RabitDataSet:

    def __init__(self, data=None, metadata=None):
        self.d = data
        self.md = metadata
        self.data = []
        self.metadata_nested = []
        self.metadata = []

    def __add__(self, other):
        rd = {}
        if self.metadata and other.metadata:
            self.metadata = self.metadata + other.metadata
            self.metadata = _check_coding_validity(metadata=self.metadata, nested=False)
            self.metadata, _ = _rename_duplicates(md=self.metadata)
            self.metadata = _set_order(self.metadata,
                                       fields={'frmCode': 'frmOrder',
                                               'fldCode': 'fldOrder',
                                               'fldParentCode': 'fldParentOrder'})
        elif other.metadata:
            self.metadata = other.metadata

        if self.metadata_nested and other.metadata_nested:
            self.metadata_nested = self.metadata_nested + other.metadata_nested
            self.metadata_nested = _check_coding_validity(metadata=self.metadata_nested, nested=True)
            self.metadata_nested, rd = _rename_duplicates(md=self.metadata_nested)
            self.metadata_nested = _set_order(self.metadata_nested,
                                              fields={'frmCode': 'frmOrder',
                                                      'fldCode': 'fldOrder',
                                                      'fldParentCode': 'fldParentOrder'})
        elif other.metadata_nested:
            self.metadata_nested = other.metadata_nested

        if self.data and other.data:
            self.data = self.data + other.data
            self.data = _sync_data_metadata(obvs=self.data, remap_dict=rd, md=self.metadata_nested)
        elif other.data:
            self.data = other.data

    def load(self):

        if not self.md and self.d:
            self.data = self.d.parse()
            return None

        if self.md and self.d:
            # parse metadata in nested form and without duplicates
            self.metadata_nested = self.md.parse(nest_options=True, rename_duplicates=False)

            if not self.metadata_nested:
                self.data = self.d.parse()
                self.metadata_nested = _generate_metadata(self.data)
                self.metadata = self.metadata_nested.copy()
            else:
                self.data, self.metadata_nested = self.d.parse(md=self.metadata_nested)
                self.metadata = self.md.parse()

            return None

        if self.md:
            self.metadata_nested = self.md.parse(nest_options=True)
            self.metadata = self.md.parse()

        return None

    def reshape(self, shape):

        keys = []
        dd = {}
        counter = {}
        tracker = {}
        data = self.data

        if not self.data:
            warnings.warn('No data passed')

        try:
            for r in data:

                keys.append((str(r.get('pid')), int(r.get('frmCode')), r.get('fillDate')))

                # TODO: this will result in a bug where a field inside the form has code 'data'
                if r.get('data') and isinstance(r.get('data'), dict):
                    dd.update({keys[-1]: r['data']})
                else:
                    dd.update({keys[-1]: r})

                # update tracker
                if (keys[-1][0], keys[-1][1]) not in counter.keys():
                    counter[(keys[-1][0], keys[-1][1])] = 1
                else:
                    counter[(keys[-1][0], keys[-1][1])] += 1

                tracker[(keys[-1][0], keys[-1][1], keys[-1][2])] = (keys[-1][0], keys[-1][1],
                                                                    counter.get((keys[-1][0], keys[-1][1])))

        except KeyError:
            raise KeyError('One or more of minimum information fields (pid, frmCode, or fillDate) not available...')

        if shape == 'merged':
            entries = {}

            for key in keys:
                entries.update({(key[0], key[1]): max(entries.get((key[0], key[1]), ''), key[2])})

            entries_list = list(entries.keys())
            entries_list.sort()

            o = {}

            for entry in entries_list:
                this_key = (*entry, entries[entry])

                try:
                    o[this_key[0]].update(dd[this_key])
                    o[this_key[0]]['fillDate'] = max(o[this_key[0]].get('fillDate', ''), this_key[2])
                except KeyError:
                    o[this_key[0]] = {}
                    o[this_key[0]].update(dd[this_key])
                    o[this_key[0]]['fillDate'] = this_key[2]

                o[this_key[0]]['pid'] = this_key[0]

            return [oo for oo in o.values()]
        elif shape == 'duplicate merged':

            o = {}

            for key in keys:

                if key[1] != 0 and key[1] != '0':
                    rec = {f'f{key[1]}_{tracker[key][2]}_{k}': v for k, v in dd[key].items()}
                    rec.update({f'f{key[1]}_{tracker[key][2]}_fillDate': key[2]})
                else:
                    rec = dd[key]
                    rec.update({'fillDate': key[2]})

                try:
                    o[key[0]].update(rec)
                except KeyError:
                    o[key[0]] = rec

                o[key[0]]['pid'] = key[0]

            return [oo for oo in o.values()]
        elif shape == 'split':
            o = {}
            for r in dd.keys():
                dd[r].update({'pid': r[0], 'fillDate': r[2]})
                fid = r[1]
                o.setdefault(fid, []).append(dd[r])
            return o
        else:
            raise ValueError('Invalid shape requested...')


class RabitResource:

    def __init__(self, kind, source, **kwargs):
        self.kind = kind
        self.source = source
        self.reader = self.__get_resource_reader(**kwargs)
        self.parser = self.__get_resource_parser(**kwargs)

    @property
    def filters(self):
        return self.reader.filters

    def add_filters(self, filters=None, field=None, condition=None, value=None):

        if isinstance(filters, list) or isinstance(filters, tuple):
            for f in filters:
                if isinstance(f, tuple) or isinstance(f, list):
                    self.__update_filter(*f)
                if isinstance(f, dict):
                    self.__update_filter(**f)
            return None
        elif field and condition and value:
            self.__update_filter(field=field, condition=condition, value=value)
        else:
            warnings.warn('Could not update filter. Check inputs...')

    def reset_filter(self):
        self.reader.reset_filter()

    def fetch(self):
        return self.reader.fetch()

    def dump(self, fp, indent=4):
        with open(fp, 'w') as f:
            json.dump(self.fetch(), f, indent=indent)

    def parse(self, **kwargs):

        # fetch data from data source
        d = self.fetch()

        # update kwargs to include RabitResource parser arguments
        this_args = {k: self.__getattribute__(k) for k in self.parser_args}
        this_args.update(kwargs)
        kwargs = this_args

        if not d:
            warnings.warn(message='RABIT API returned None. Check settings...')
            return None

        return self.parser(d, **kwargs)

    def __update_filter(self, field, condition, value):
        self.reader.add_filter(field, condition, value)

    def __get_resource_reader(self, **kwargs):

        if self.source == 'api':
            baseurl = kwargs.get('baseurl')
            uri = kwargs.get('uri')
            route = kwargs.get('route')
            verify = kwargs.get('verify', False)
            parameters = kwargs.get('parameters', dict())
            headers = kwargs.get('headers')
            paginated = kwargs.get('paginated', False)
            return RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route=route, verify=verify, parameters=parameters,
                                         headers=headers, paginated=paginated)
        elif self.source == 'json-file':
            fp = kwargs.get('fp')
            encoding = kwargs.get('encoding', 'utf-8')
            return RabitReaderJSONFileAdapter(fp=fp, encoding=encoding)

        elif self.source == 'json':
            raise NotImplementedError('Direct JSON not currently supported')

        else:
            raise ValueError(f'Unrecognized source type: {self.source}...')

    def __get_resource_parser(self, **kwargs):

        if self.kind == 'metadata':

            # set all necessary data for this parser
            self.parser_args = ['fid_path', 'json_path', 'include_html', 'nest_options', 'rename_duplicates',
                                'frm_name_path', 'frm_desc_path']
            self.fid_path = kwargs.get('fid_path', 'id')
            self.json_path = kwargs.get('json_path', 'json')
            self.include_html = kwargs.get('include_html', False)
            self.nest_options = kwargs.get('nest_options', False)
            self.rename_duplicates = kwargs.get('rename_duplicates', True)
            self.frm_name_path = kwargs.get('frm_name_path', 'surveyName')
            self.frm_desc_path = kwargs.get('frm_desc_path', 'surveyDescription')
            return _parse_metadata

        elif self.kind == 'data':

            # set all necessary data for this parser
            self.parser_args = ['pid_path', 'fid_path', 'fill_date_path', 'json_path', 'usefields']

            try:
                self.pid_path = kwargs['pid_path']
                self.fill_date_path = kwargs['fill_date_path']
            except KeyError:
                raise KeyError('both pid_path and fill_date_path should be supplied for data resource...')

            self.fid_path = kwargs.get('fid_path')
            self.json_path = kwargs.get('json_path')
            self.usefields = kwargs.get('usefields')
            self.timeres = kwargs.get('timeres', 'second')

            return _parse_data
        else:
            raise ValueError(f'Unrecognized resource type: {self.kind}')
