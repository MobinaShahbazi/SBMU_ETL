import logging
from abc import ABC, abstractmethod
from rabitpy_dev_phase_info.errors import APINotAvailableError
import requests
import json
import os
import re
from sqlalchemy import create_engine
from rabitpy_dev_phase_info.utils import timing
import math

requests.packages.urllib3.disable_warnings()


class RabitReaderBaseAdapter(ABC):

    @abstractmethod
    def fetch(self):
        pass

    @property
    @abstractmethod
    def filters(self):
        pass

    @abstractmethod
    def add_filter(self):
        pass

    @abstractmethod
    def reset_filters(self):
        pass


class RabitReaderAPIAdapter(RabitReaderBaseAdapter):

    def __init__(self, baseurl, uri, route, parameters,  method='GET', verify=False, headers=None, paginated=False,
                 page_size=500, page_limit=math.inf ):
        self.url = f'{baseurl}/{uri}/{route}'
        self.parameters = parameters
        self.verify = verify
        self.headers = headers
        self.method = method
        self.paginated = paginated
        self.page_size = page_size
        self.page_limit = page_limit
        self._filters = {}
        self._req = None

    @property
    def req(self):

        if self.method == 'POST':
            # TODO: This requires adding a body to the request and other post options which may be needed
            self._req = requests.Request(self.method, self.url, params=self.parameters, headers=self.headers).prepare()
        elif self.method == 'GET':
            self._req = requests.Request(self.method, self.url, params=self.parameters, headers=self.headers).prepare()

        return self._req

    @property
    def filters(self):
        return self._filters

    def add_filter(self, field, condition, value):

        if not self.filters:
            self.filters['filterslength'] = 1
        else:
            self.filters['filterslength'] += 1

        n = self.filters['filterslength'] - 1
        this_filter = {f'filterdatafield{n}': field, f'filtercondition{n}': condition, f'filtervalue{n}': value}

        self.filters.update(this_filter)
        self.parameters.update(self.filters)

    def reset_filters(self):

        self._filters = {}
        pattern =  'filterslength|filterdatafield\d+|filtercondition\d+|filtervalue\d+'
        target_keys = list(filter(lambda x: re.fullmatch(pattern, x), self.parameters.keys()))
        for k in target_keys:
            self.parameters.pop(k)

    @timing
    def fetch(self):

        retry_strategy = requests.packages.urllib3.util.retry.Retry(connect=1)
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)

        s = requests.Session()
        s.mount(prefix='https://', adapter=adapter)
        s.mount(prefix='http://', adapter=adapter)

        has_next = True
        if self.paginated:
            pagenum = 0
            fetched_content = []

            print(f'Sending Paginated HTTP request {self.req.method} {self.url}')
            while has_next:
                self.parameters.update({'pagesize': self.page_size, 'pagenum': pagenum})
                res = s.send(self.req, verify=self.verify)

                if res.status_code != 200:
                    raise APINotAvailableError(
                        f'Error getting data from API. Process exited with status code: {res.status_code}')
                else:
                    try:
                        fetched_data = json.loads(res.text)
                        fetched_content += fetched_data['content']
                        print(f'Fetched {pagenum} of {fetched_data["totalPages"]}')
                        if pagenum == fetched_data['totalPages']:
                            fetched_data['content'] = fetched_content
                            has_next = False
                        else:
                            pagenum += 1

                    except json.decoder.JSONDecodeError:
                        if not res.text:
                            raise ValueError(f'Empty response recieved while fetching from {res.url}...')
                        else:
                            raise ValueError(f'Could not decode response text: {res.text[:20]}...')
            return fetched_data
        else:
            print(f'Sending HTTP request {self.req.method} {self.url}')
            res = s.send(self.req, verify=self.verify)

            if res.status_code != 200:
                raise APINotAvailableError(
                    f'Error getting data from API. Process exited with status code: {res.status_code}')
            else:
                try:
                    return json.loads(res.text)
                except json.decoder.JSONDecodeError:
                    if not res.text:
                        raise ValueError(f'Empty response recieved while fetching from {res.url}...')
                    else:
                        raise ValueError(f'Could not decode response text: {res.text[:20]}...')


class RabitReaderJSONFileAdapter(RabitReaderBaseAdapter):

    def __init__(self, fp=None, encoding='utf-8'):
        self.fp = fp
        self.encoding = encoding

    def fetch(self):

        if not os.path.isfile(self.fp):
            raise FileNotFoundError(f'The specified path {self.fp} is not a file...')

        if not os.path.exists(self.fp):
            raise FileNotFoundError(f'The specified path {self.fp} does not exist...')

        with open(self.fp) as f:
            return json.load(f)

    @property
    def filters(self):
        pass

    def add_filter(self):
        pass

    def reset_filters(self):
        pass


class RabitReaderJSONObjAdapter(RabitReaderBaseAdapter):

    def __init__(self, obj=None):
        self.obj = obj

    def fetch(self):

        if isinstance(self.obj, str):
            return json.loads(self.obj)
        elif isinstance(self.obj, dict) or isinstance(self.obj, list):
            return json.loads(json.dumps(self.obj))

    @property
    def filters(self):
        pass

    def add_filter(self):
        pass

    def reset_filters(self):
        pass


class RabitDatabaseAdapter(RabitReaderBaseAdapter):

    def __init__(self, url, query, connect_args={}):
        self.engine = create_engine(url, connect_args=connect_args)
        self.query = query

    def fetch(self):
        return [dict(x) for x in self.engine.execute(self.query).mappings().all()]

    @property
    def filters(self):
        pass

    def add_filter(self):
        pass

    def reset_filters(self):
        pass

