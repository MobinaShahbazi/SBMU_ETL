from abc import abstractmethod
from rabitpy.errors import APINotAvailableError
from requests import Request, Session
import json
import os
import warnings


class RabitReaderBaseAdapter:

    @abstractmethod
    def fetch(self):
        pass


class RabitReaderAPIAdapter(RabitReaderBaseAdapter):

    def __init__(self, baseurl, uri, route, parameters, verify=False, headers=None):
        self.url = f'{baseurl}/api/{uri}/{route}'
        self.filters = {}
        self.parameters = parameters
        self.verify = verify
        self.headers = headers

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

        s = Session()
        res = s.send(self.req, verify=self.verify)

        # TODO: Add error handling for malformed json
        if res.status_code != 200:
            raise APINotAvailableError(
                f'Error getting data from API. Process exited with status code: {res.status_code}')
        else:
            try:
                return json.loads(res.text)
            except json.decoder.JSONDecodeError:
                warnings.warn(f'Could not decode response text {res.text[:20]}...')
                return None

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
