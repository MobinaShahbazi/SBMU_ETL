import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import re
import warnings
from datetime import datetime
from collections import OrderedDict
from .validity import _check_coding_validity
import pandas as pd


def _parse_metadata(d, fid_path, json_path, include_html=False, nest_options=False, rename_duplicates=True,
                    frm_name_path='surveyName', frm_desc_path='surveyDescription'):

    # This is the main function for parsing metadata

    if not json_path:
        return None

    metadata = []

    if not isinstance(d, list):
        d = [d]

    for qnr in d:

        fid = qnr[fid_path] if fid_path else 0

        # check and return exception if metadata json does not contain 'json' key
        qnrjson = qnr.get(json_path)

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
                    if element['type'] == 'html' and not include_html:
                        continue

                    metadata += _set_properties(_qnr=qnr,
                                                _element=element,
                                                _choices=_extract_choices(_el=element),
                                                _digit=nest_options,
                                                _fid=fid_path,
                                                _frm_name=frm_name_path,
                                                _frm_desc=frm_desc_path)

                # Add errors to output dict where there are exceptions
                except Exception as e:

                    print(f'error in processing {qnr.get(fid_path)} {element.get("name")}')

                    metadata.append({'fldCode': element.get('name'),
                                     'fldTitle': element.get('title'),
                                     'frmCode': qnr.get(fid_path) if fid_path else 0,
                                     'frmName': qnr.get(frm_name_path),
                                     'frmDesc': qnr.get(frm_desc_path),
                                     'frmOrder': qnr.get('sortOrder'),
                                     'error': e})
                    raise

    if metadata:
        metadata = _check_coding_validity(metadata=metadata, nested=nest_options)
    else:
        return None

    if rename_duplicates:
        metadata, _ = _rename_duplicates(metadata)

    metadata = _set_order(metadata, {'frmCode': 'frmOrder', 'fldCode': 'fldOrder', 'fldParentCode': 'fldParentOrder'})

    return metadata


def _set_properties(_qnr=None, _element=None, _choices=None, _digit=False, _fid=None, _frm_name='surveyName',
                    _frm_desc='surveyDescription'):

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
            # rec_element.update({'name': '{}_{}'.format(_element.get('name', ''), item.get('name')),
            #                     'type': 'text',
            #                     'visibleIf': _element.get('visibleIf'),
            #                     'fldParentCode': _element.get('name', ''),
            #                     'fldParentTitle': _element.get('title', '')})

            rec_element.update({'name': '{}_{}'.format(_element.get('name', ''), item.get('name')),
                                'type': 'text',
                                'visibleIf': _element.get('visibleIf'),
                                'fldParentCode': _element.get('name', ''),
                                'fldParentTitle': _translation_handler(s=_element, target='title', alt='')})

            # send the element back to the function and update properties
            this = _set_properties(_qnr=_qnr, _element=rec_element, _digit=_digit, _fid=_fid)

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
                # fldtmp['title'] = '{} - {}'.format(_element.get('title', _element.get('name')),
                #                                    fld.get('text', fld.get('name')))
                fldtmp['title'] = '{} - {}'.format(_translation_handler(s=_element, target='title', alt=_element.get('name')),
                                                   _translation_handler(s=fld, target='text', alt=fld.get('name')))

            elif isinstance(fld, str):
                fldtmp['name'] = '{}_{}'.format(_element.get('name'), fld)
                fldtmp['title'] = fld

            else:
                raise TypeError(f'Matrix row type is {type(fld)} and not dict or string...')

            fldtmp.update({'choices': optAx,
                           'type': 'radiogroup',
                           'visibleIf': _element.get('visibleIf'),
                           'fldParentCode': _element.get('name'),
                           'fldParentTitle': _translation_handler(s=_element, target='title', alt=_element.get('name'))
                           })

            # send the element back to the function and update properties
            this = _set_properties(_qnr=_qnr,
                                   _element=fldtmp,
                                   _choices=_extract_choices(_el=fldtmp),
                                   _digit=_digit,
                                   _fid=_fid)

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

            ttl = _translation_handler(s=_element, target='title', alt=_element.get('name'))
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
                # TODO: Check visibleIf condition for column and rows and implenet in necessary
                # fld['visibleIf'] = _element.get('visibleIf', None)

                ttl = _translation_handler(s=col, target='title', alt=col.get('name'))
                fld['title'] = f"{parentFldTitle} - {ttl}"
                fld['name'] = f"{parentFldCode}_{col.get('name')}"
                fld['fldParentCode'] = parentFldCode
                fld['fldParentTitle'] = parentFldTitle

                # send the element back to the function and update properties
                this = _set_properties(_qnr=_qnr,
                                       _element=fld,
                                       _choices=_extract_choices(_el=col),
                                       _digit=_digit,
                                       _fid=_fid)

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
            el['fldParentTitle'] = _translation_handler(s=el, target='title', alt=el.get('name'))

            this = _set_properties(_qnr=_qnr,
                                   _element=el,
                                   _digit=_digit,
                                   _fid=_fid,
                                   _choices=_extract_choices(_el=el))

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

        parentFldTitle = f"{_translation_handler(s=_element, target='title', alt=_element.get('name'))}"
        parentFldCode = f"{_element.get('name')}_r1"
        fld = {}

        for col in optAx:
            fld['type'] = col.get('cellType', 'dropdown')

            # TODO: Check visibleIf condition for column and rows and implement if necessary
            # fld['visibleIf'] = _element.get('visibleIf', None)

            fld['title'] = f"{parentFldTitle} - {_translation_handler(s=col, target='title', alt=col.get('name'))}"
            fld['name'] = f"{parentFldCode}_{col.get('name')}"

            # send the element back to the function and update properties
            this = _set_properties(_qnr=_qnr,
                                   _element=fld,
                                   _choices=_extract_choices(_el=col),
                                   _fid=_fid,
                                   _digit=_digit)

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
            el['name'] = '{}_r1_{}'.format(_element.get('name'), el.get('name'))
            this = _set_properties(_qnr=_qnr,
                                   _element=el,
                                   _digit=_digit,
                                   _fid=_fid,
                                   _choices=_extract_choices(_el=el))

            if isinstance(this, dict):
                this['elementType'] = f'paneldynamic - {this["elementType"]}'
            elif isinstance(this, list):
                for item in this:
                    item['elementType'] = f'paneldynamic - {item["elementType"]}'

            properties = updater(this.copy())
        return properties

    # set common properties, the type property is set here once and if necessary it will be reset in later steps
    this['frmCode'] = _qnr.get(_fid) if _fid else 0
    this['frmName'] = _qnr.get(_frm_name, '').strip() if _fid else 'registry'
    this['frmDesc'] = _qnr.get(_frm_desc, '').strip()
    this['fldCode'] = _element.get('name').strip()
    this['fldTitle'] = _translation_handler(_element, 'title', _element.get('name')).strip()
    # this['fldTitle'] = _element.get('title', this['fldCode']).strip()
    this['fldParentCode'] = (_element.get('fldParentCode') if _element.get('fldParentCode')
                             else this['fldCode']).strip()
    this['fldParentTitle'] = (_element.get('fldParentTitle') if _element.get('fldParentTitle')
                              else this['fldTitle']).strip()
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

    elif this.get('elementType') in ['radiogroup', 'dropdown', 'rating']:
        this.update({'dType': 'category'})

    elif this.get('elementType') in ['checkbox']:
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
                # chText = _choice.get('text', _choice.get('value')).strip()
                chText = _translation_handler(_choice, 'text', _choice.get('value'))
            else:
                chValue = str(_choice).strip()
                chText = _choice.strip()

            # update name and field code of checkbox type quesitons, update and continue
            if this.get('elementType') == 'checkbox':
                this.update({'fldTitle': '{} - {}'.format(parentFldTitle, chText),
                             'fldCode': '{}_{}'.format(parentFldCode, chValue)})

                this.update({'optVal': chValue, 'optText': chText})
                updater(this.copy())
                continue

            # if question was not checkbox type, then either append to opt for nested or update row and move on
            if _digit:
                opt.append({'value': chValue, 'text': chText})
            else:
                this.update({'optVal': chValue, 'optText': chText})
                updater(this.copy())

        # TODO: opt is added to this because later it tries to pop it and if nested option is on it fails.
        #  Probably removable but should be checked.

        this.update({'opt': opt})

        if opt:
            updater(this.copy())

    else:
        updater(this.copy())

    # Add 'other' option if it is active
    if _element.get('hasOther'):
        if _digit:
            this.pop('opt')
        else:
            this.pop('optVal')
            this.pop('optText')
        this.update({'dType': 'str', 'elementType': 'text', 'fldCode': '{}-{}'.format(_element['name'], 'Comment')})
        this.update({'fldTitle': '{} - {}'.format(parentFldTitle, this.get('otherText', 'other'))})
        updater(this.copy())

    return properties


def _extract_choices(_el):

    if _el.get('choices', _el.get('rateValues')):
        return _el.get('choices', _el.get('rateValues'))

    if _el.get('choicesByUrl'):
        url = _el.get('choicesByUrl').get('url').split('&')[0]
        value_name = _el.get('valueName', 'code')
        title_name = _el.get('titleName', 'title')

        retry_strategy = Retry(connect=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount(prefix='https://', adapter=adapter)
        session.mount(prefix='http://', adapter=adapter)

        try:
            response = session.get(url, verify=False, timeout=2)
        except ConnectionError as e:
            warnings.warn(f'Failed to extract choices from {url}. Check if address exists.')
            return []

        r = json.loads(response.text, strict=False)
        c = []

        try:
            for item in r:
                item['value'] = item.pop(value_name)
                item['text'] = item.pop(title_name)
                c.append(item)
        except Exception as e:
            warnings.warn(f'An unexpected exception occured when reading choice data: {e}')

        return c


def _rename_duplicates(md):

    # TODO: Remove conversions to dataframe when all is pandas based.

    # propagate renamed codes in metadata
    # get all list elements marked as duplicates
    mdf = pd.DataFrame().from_records(md)
    dupcols = ['frmCode', 'fldCode']
    tmp = mdf.drop_duplicates(subset=dupcols)
    dup = tmp.loc[tmp.duplicated(subset='fldCode'), 'fldCode']

    del tmp

    # get boolean array for duplicated values
    ix = mdf['fldCode'].isin(dup)

    # Assign new names for duplicated values
    mdf.loc[ix, 'fldCodeR'] = 'frm' + \
                              mdf.loc[ix, 'frmCode'].astype(str) \
                              + '_' + \
                              mdf.loc[ix, 'fldCode'].astype(str)

    # Create renaming dictionary in {frmCode: {field1: field1_new_name}} format
    rd = dict.fromkeys(mdf.loc[ix, 'frmCode'].unique())

    for frm in rd.keys():
        ixx = mdf['frmCode'] == frm
        rd[frm] = dict(zip(mdf.loc[ix & ixx, 'fldCode'], mdf.loc[ix & ixx, 'fldCodeR']))
        rdvc = dict(zip(mdf.loc[ix & ixx, 'fldCode'].apply(lambda x: f"{ {x} }".replace("'", "").replace(' ', '')),
                        mdf.loc[ix & ixx, 'fldCodeR'].apply(lambda x: f"{ {x} }".replace("'", "").replace(' ', ''))
                        ))
        mdf.loc[ix & ixx, 'fldCode'] = mdf.loc[ix & ixx, 'fldCodeR']

        for k, v in rdvc.items():
            mdf.loc[ixx, 'visibleCondition'] = mdf.loc[ixx, 'visibleCondition'].fillna('').str.replace(k, v, regex=True)
            mdf.loc[ixx, 'expression'] = mdf.loc[ixx, 'expression'].fillna('').str.replace(k, v, regex=True)

    mdf = mdf.drop(columns='fldCodeR')
    md = mdf.to_dict(orient='records')
    return md, rd


def _add_dynamics_to_metadata(md, dynamics_counter):

    # get all dynamic fields in the metadata
    dynamic_fields = list(filter(lambda x: x['elementType'].startswith('matrixdynamic') or
                                           x['elementType'].startswith('paneldynamic'), md))

    # for each record in dynamic fields add them to the nested metadata
    for fld in dynamic_fields:
        fc = dynamics_counter.get(fld['frmCode'])

        try:
            k = list(filter(lambda x: fld['fldCode'].startswith(f'{x}_r'), fc))
        except:
            continue

        try:
            n = fc[k[0]]
            for i in range(2, n + 1):
                this_fld = fld.copy()
                this_fld['fldCode'] = re.sub(f'^{k[0]}_r\d+_', f'{k[0]}_r{i}_', fld['fldCode'])
                md.append(this_fld)
        except IndexError:
            pass

    return md


def _set_order(md, fields):

    last = {k: -1 for k in fields}
    counter = {k: 0 for k in fields}

    for rec in md:
        for k, v in fields.items():
            if last[k] == -1 or last[k] != rec[k]:
                counter[k] += 1
                last[k] = rec[k]

            rec.update({v: counter[k]})

    return md


def _parse_data(d, pid_path, fill_date_path, fid_path=None, json_path=None, usefields=None, timeres='second', md=None):

    tmp = {'pid': pid_path, 'frmCode': fid_path, 'json': json_path, 'fillDate': fill_date_path}
    index_fields = {k: v for k, v in tmp.items() if tmp[k]}

    # check data path exists and get raw flattened data
    if isinstance(d, dict) and d.get('content') and d['content'] and isinstance(d['content'], list):
        observations, dynamics_counter, ext_fields = _get_raw_flat_data(d['content'], index_fields, usefields, timeres)
    else:
        warnings.warn('Specified data path not found or is empty, check path and try again...')
        if not md:
            return None
        else:
            return None, md

    # return raw flattened data if no metadata is provided
    if not md:
        return list(observations.values())

    # Add ext_fields parameters to metadata
    mdf = pd.DataFrame().from_records(md)
    frm_details = mdf.drop_duplicates(subset=['frmCode'])
    ext_fields_dct = dict.fromkeys(ext_fields)

    for k, v in index_fields.items():

        try:
            if k != 'json':
                ext_fields_dct[k] = ext_fields_dct.pop(v)
            else:
                ext_fields_dct.pop(v)
        except KeyError:
            raise

    include_cols = ['frmCode', 'frmName', 'frmDesc', 'frmOrder']
    for i, frm_detail in frm_details.iterrows():
        this_meta = _generate_metadata(d=[ext_fields_dct], **frm_detail[include_cols].to_dict())
        mdf = mdf.append(this_meta)

    md = mdf.to_dict(orient='records')

    # if metadata is given, then add dynamic element counts to metadata and rename duplicated values
    md = _add_dynamics_to_metadata(md, dynamics_counter)
    md, remap_dict = _rename_duplicates(md)
    observations = _sync_data_metadata(obvs=observations, md=md, remap_dict=remap_dict)

    return observations, md


def _get_raw_flat_data(obvs, index_fields, usefields, timeres='second'):


    # add required fields to usefields
    if not usefields:
        ext_fields = list(index_fields.values())
    elif usefields == 'all':
        ext_fields = list(obvs[0].keys())
    elif isinstance(usefields, list):
        ext_fields = list(set(usefields + list(index_fields.values())))
    else:
        raise TypeError(f'Expected usefields to be of type list or string "all" but got {type(usefields)}')

    observations = {}
    dynamics_counter = {}

    for obv in obvs:

        # filter only fields that we need
        try:
            obv = {k: obv[k] for k in ext_fields}
        except KeyError:
            raise

        # rename fields in content
        obv = _rename_fields(obv, index_fields)

        pid = obv['pid']
        fd = obv['fillDate']

        # if time resolution argument is given, then the data will be organized by createdDate
        tfmt = {'day': '%Y-%m-%d', 'hour': '%Y-%m-%d %H', 'minute': '%Y-%m-%d %H:%M', 'second': '%Y-%m-%d %H:%M:%S'}

        try:
            fd = datetime.strptime(fd, '%Y-%m-%d %H:%M:%S.%f') \
                .strftime(tfmt.get(timeres))
        except ValueError:
            pass

        # If data is not related to a json
        if not index_fields.get('frmCode'):
            fid = 0
            obv['frmCode'] = fid
        else:
            fid = obv['frmCode']

        k = (pid, fid, fd)

        if index_fields.get('json'):

            rd = obv.pop('json')

            if rd:

                try:
                    d, this_counter = _flatten_json('', json.loads(rd, strict=False), counter={})
                except ValueError:
                    warnings.warn(f'Record {k} contains an invalid JSON. Skipping...')
                    d = {}

                d.update(obv)

                if this_counter:
                    dynamics_counter[fid] = \
                        {k: max(dynamics_counter.get(fid, {}).get(k, 0), this_counter.get(k, 0)) for k in this_counter}
            else:
                d = obv

        else:
            d = obv

        observations.setdefault(k, {}).update(d)

    # Return nothing if project does not have data
    if not observations:
        warnings.warn(message='This project has no data...')
        return None

    return observations, dynamics_counter, ext_fields


def _sync_data_metadata(obvs, remap_dict=None, md=None):

    observations = []

    if md:
        # get fields in forms in the shape of {frmCode1: [fldCode1, fldCode2, ...]}
        field_codes_dict = dict()
        for row in md:
            field_codes_dict.setdefault(row['frmCode'], []).append(row['fldCode'])

    # obvs is in {(pid, frmId, fillDate): {response}} format
    for item in obvs:

        if isinstance(obvs, dict):
            k = item
            obv = obvs[item]
        elif isinstance(obvs, list):
            k = (item['pid'], item['frmCode'], item['fillDate'])
            obv = item

        # create default dictionary
        this_record = {'pid': k[0], 'frmCode': k[1], 'fillDate': k[2]}

        if remap_dict:
            # replace duplicated keys for obv
            if remap_dict.get(k[1], None):
                for fldId, fldIdRp in remap_dict.get(k[1]).items():
                    obv[fldIdRp] = obv.pop(fldId, None)

        if md:
            # update record with metadata
            try:
                this_record.update({kk: str(obv.get(kk)) if obv.get(kk) is not None else None
                                    for kk in field_codes_dict[k[1]]})
            except KeyError:
                warnings.warn(message=f'Form code: {k[1]} not found in current metadata. Skipping...')
                pass
        else:
            this_record.update(obv)

        observations.append(this_record)

    return observations


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


def _rename_fields(dct, rndct):

    for k, v in rndct.items():

        if k in dct.keys():
            continue

        try:
            dct[k] = dct.pop(v)
        except KeyError:
            raise KeyError(f'Missing field {k} while renaming fields...')

    return dct


def _generate_metadata(d, **kwargs):

    meta_common = {'frmCode': kwargs.get('frmCode', 0), 'frmName': kwargs.get('frmName', 'registry'),
                   'frmDesc': kwargs.get('frmDesc'), 'frmOrder': kwargs.get('frmOrder'),
                   'fldParentCode': kwargs.get('fldParentCode'),
                   'visibleCondition': kwargs.get('fldParentCode'), 'dType': kwargs.get('fldParentCode', 'str'),
                   'fldOrder': kwargs.get('fldOrder', 0), 'fldParentOrder': kwargs.get('fldParentOrder', 0),
                   'elementType': kwargs.get('elementType', 'text'), 'warning': kwargs.get('warnings', []),
                   'optVal': kwargs.get('optVal'), 'optText':  kwargs.get('optText'), 'opt':  kwargs.get('opt')}

    # pid_meta = meta_common.copy()
    # pid_meta.update({'fldTitle': 'کد',
    #                  'fldCode': 'pid',
    #                  'dType': 'numeric'})
    #
    # generated_metadata = [pid_meta]
    # handled_flds = ['pid']

    # Create metadata for selected fields
    if d:

        flds = []
        for rec in d:
            flds += list(rec.keys())

        flds = list(set(flds))
        generated_metadata = []

        for field in flds:
            this_field_meta = meta_common.copy()
            this_field_meta.update({'fldTitle': field, 'fldCode': field, 'dType': 'str'})
            generated_metadata.append(this_field_meta)
    else:
        generated_metadata = []

    return generated_metadata


def _translation_handler(s, target, alt):

    if target in s:
        if isinstance(s[target], dict):
            return s[target].get('fa', s[target].get('default'))
        else:
            return s[target]
    else:
        return alt




