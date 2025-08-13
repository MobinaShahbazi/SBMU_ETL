from abc import abstractmethod
from rabitpy.errors import APINotAvailableError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests import Request, Session
import requests
import json
import os
import warnings


class RabitReaderBaseAdapter:

    @abstractmethod
    def fetch(self):
        pass


class RabitReaderAPIAdapter(RabitReaderBaseAdapter):

    def __init__(self, baseurl, uri, route, parameters, verify=False, headers=None, paginated=False):
        self.url = f'{baseurl}/api/{uri}/{route}'
        self.filters = {}
        self.parameters = parameters
        self.verify = verify
        self.headers = headers
        self.paginated = paginated

    @property
    def req(self):
        return Request('POST', self.url, params=self.parameters, headers=self.headers).prepare()

    def add_filter(self, field, condition, value):

        if not self.filters:
            self.filters['filterslength'] = 1
        else:
            self.filters['filterslength'] += 1

        n = self.filters['filterslength'] - 1
        this_filter = {f'filterdatafield{n}': field, f'filtercondition{n}': condition, f'filtervalue{n}': value}

        self.filters.update(this_filter)
        self.parameters.update(self.filters)

    def reset_filter(self):
        self.filters = {}

    def fetch(self):

        retry_strategy = requests.packages.urllib3.util.retry.Retry(connect=1)
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)

        s = requests.Session()
        s.mount(prefix='https://', adapter=adapter)
        s.mount(prefix='http://', adapter=adapter)

        has_next = True
        if self.paginated:
            pagesize = 500
            pagenum = 0
            fetched_content = []

            print(f'Sending Paginated HTTP request {self.req.method} {self.url}')
            while has_next:
                self.parameters.update({'pagesize': pagesize, 'pagenum': pagenum})
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

        with open(self.fp, encoding=self.encoding) as f:
            return json.load(f, encoding=self.encoding)
