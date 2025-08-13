from copy import deepcopy


def _get_idx(index_fields, use_fields, available_fields):

    if isinstance(index_fields, str):
        index_fields = [index_fields]
    elif not isinstance(index_fields, list):
        raise TypeError(f'index_fields must be either a list or a string')

    if use_fields is None:
        use_fields = []
    elif isinstance(use_fields, str):
        if use_fields != 'all':
            use_fields = [use_fields]
        else:
            use_fields = available_fields
    elif not isinstance(use_fields, list):
        raise TypeError(f'use_fields must be either a list or a string')

    use_fields = set(use_fields).difference(set([x['name'] for x in index_fields]))

    idx = {}

    for fields, is_index in [(index_fields, True), (use_fields, False)]:
        for item in fields:
            if isinstance(item, str):
                idx[item] = {'index': is_index}
            elif isinstance(item, dict):
                try:
                    name = item['name']
                except KeyError:
                    raise KeyError('index fields given in dictionaries must have "name" property')
                idx[name] = deepcopy(item)
                idx[name].update({'index': is_index})
                idx[name].pop('name')
            else:
                raise TypeError('fields must be either sting or dictionary')
    return idx


def _flatten_json(key, value, counter, sep='_'):
    out = {}

    if isinstance(value, list):
        for item in value:

            if not item:
                continue

            if isinstance(item, dict):
                if counter.get(key):
                    counter[key] = counter[key] + 1
                else:
                    counter[key] = 1
                tmp, counter = _flatten_json(f'{key}', item, counter=counter)
                out.update(tmp)
            else:
                out.update({f'{key}{sep}{item}': True for item in value})
        return out, counter

    if isinstance(value, dict):
        for k, v in value.items():

            if k == 'pos':
                continue

            s = '' if key == '' else sep
            if counter.get(key, 0) == 0:
                tmp, counter = _flatten_json(f'{key}{s}{k}', v, counter=counter)
                out.update(tmp)
            else:
                tmp, counter = _flatten_json(f'{key}{s}r{counter[key]}{s}{k}', v, counter=counter)
                out.update(tmp)
        return out, counter

    return {key: value}, counter


def _translation_handler(s, target, alt):

    if target in s:
        if isinstance(s[target], dict):
            return s[target].get('fa', s[target].get('default'))
        else:
            return s[target]
    else:
        return alt
