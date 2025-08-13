import re
import pandas as pd

def _add_dynamics_to_metadata(md, dynamics_counter):

    # get all dynamic fields in the metadata
    ix = (md['elementType'].str.startswith('matrixdynamic')) | (md['elementType'].str.startswith('paneldynamic'))
    dynamic_fields = md.loc[ix].to_dict(orient='records')

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
                this_fld['fldParentCode'] = re.sub(f'^{k[0]}_r\d+_', f'{k[0]}_r{i}_', fld['fldParentCode'])
                md = pd.concat([md, pd.Series(this_fld).to_frame().T])
        except IndexError:
            pass
        except ValueError:
            pass

    return md


def _set_order(md, fields):
    # fields: a dictionary of the form {old_column: new_column}
    # this function sets numeric order for categories in old_column and saves it in new_column based on order in md

    md[list(fields.values())] = (md[list(fields.keys())].shift(1) != md[list(fields.keys())]).cumsum()

    return md


def _rename_dict(d, rndct):
    for k, v in rndct.items():
        try:
            d[v] = d.pop(k)
        except KeyError:
            pass
    return d


def _replace_set(s, rndct):
    if rndct:
        return s.difference(set(rndct.keys())).union(set(rndct.values()))
    else:
        return s


def _sync_data_metadata(obvs, remap_dict=None, md=None):

    if isinstance(md, list):
        # get fields in forms in the shape of {frmCode1: [fldCode1, fldCode2, ...]}
        mdf = pd.DataFrame().from_records(md)
    elif isinstance(md, pd.DataFrame):
        mdf = md
    else:
        raise TypeError(f'Metadata should either be in Pandas DataFrame or List form, got {type(md)}')


    # obvs is DataFrame with index_fields + json
    # obvsgbo = obvs.groupby(by=['frmCode', 'phaseId'])
    obvsgbo = obvs.groupby(by=['frmCode'])
    obvslst = {}

    # Process responses for each form in each phase separately
    for name, obvg in obvsgbo:
        # extract field codes from metadata
        # this_form_fields = mdf.loc[(mdf['frmCode'] == name[0]) & (mdf['phaseId'] == name[1])] \
        #     .sort_values('fldOrder')['fldCode'].unique()

        this_form_fields = mdf.loc[(mdf['frmCode'] == name)] \
            .sort_values('fldOrder')['fldCode'].unique()

        # Define an empty dataframe with all necessary fields
        this_df = pd.DataFrame(columns=this_form_fields)

        # Create a dataframe from current records and apply remapping dictionary for this form
        tmp = this_df.append(obvg['json'].apply(pd.Series).rename(columns=remap_dict.get(name, {})))

        # Update observations list
        obvslst.update({name: pd.concat([obvg.drop(columns=['json']), tmp], axis=1)})

    return obvslst


def _sync_data_phase(o, ph):
    p = pd.DataFrame().from_records(ph)
    p['phaseAlias'] = 'p' + p['phaseOrder'].astype(str)
    # p = p.drop('surveyIds').merge(p['surveyIds'].explode(), left_index=True, right_index=True)

    o = o.merge(p[['id', 'phaseAlias']], left_on='phaseId', right_on='id')
    o['json'] = o.apply(lambda x: {k + '_' + x['phaseAlias']: v for k, v in x['json'].items()}, axis=1)
    o.drop(columns=['phaseId', 'phaseAlias', 'id'], inplace=True)

    return o

