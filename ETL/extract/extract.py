# from common.utils import get_project_data, get_entry_registry_data, get_response_data, get_entry_path_data
from wsgiref import headers

from rabitpy_dev_phase_info.io.resources import RabitData, RabitMetadata, RabitDataset, RabitProject
from rabitpy_dev_phase_info.io.adapters import RabitReaderAPIAdapter
import khayyam as kym

from rabitpy.io.rdata import RabitResource

def gregorian_to_jalali(x, format='%Y-%m-%d'):
    if not isinstance(x, str):
        raise TypeError('Input date should be a string.')
    return kym.JalaliDate.strptime(x, format).todate()


def get_project_data():
    head = {'Authorization': 'Basic UkBiaVQwMTpWaXNUQFJAYml0'}
    # project_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='project.do', parameters=params, headers=headers)
    project_reader = RabitResource(kind='metadata', source='api', baseurl=baseurl, uri=uri, route='project.do', parameters=params,
                        json_path='registryQuestioneeSurveyJson', fid_path=None, headers=head)

    project_resource = RabitProject(source='api', reader=project_reader)
    project_resource.parse(cache=True)
    return project_resource


if __name__ == '__main__':
    source = 'api'
    # baseurl = 'https://panel.rabit.ir/api'
    # uri = '6fabec64-081f-4b40-834a-f3a94b348a73'
    baseurl = 'https://disregapi.sbmu.ac.ir'
    uri = '464b3f1a-73d1-460b-af45-f628e01571f7'
    params = {'unpaged': 1}
    head = {'Authorization': 'Basic UkBiaVQwMTpWaXNUQFJAYml0'}
    # project_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='project.do', parameters=params, headers=headers)
    q = RabitResource(kind='data', source='api', baseurl=baseurl, uri=uri, route='responds.do', parameters=params,
                      pid_path='id', fill_date_path='createdDate', usefields='all',
                      headers={'Authorization': 'Basic UkBiaVQwMTpWaXNUQFJAYml0'})

    qmd = RabitResource(kind='metadata', source='api', baseurl=baseurl, uri=uri, route='project.do', parameters=params,
                        json_path='registryQuestioneeSurveyJson', fid_path=None, headers={'Authorization': 'Basic UkBiaVQwMTpWaXNUQFJAYml0'})


    data_index_fields = [{'name': 'id', 'alias': 'pid', 'dtype': 'int', 'metadata': {}},
                         {'name': 'surveyId', 'alias': 'frmCode', 'default': 0, 'dtype': 'int'},
                         {'name': 'createdDate', 'alias': 'fillDate'},
                         {'name': 'pathRespondId', 'dtype': 'float'},
                         {'name': 'questionerId', 'alias': 'questioner', 'default': ''},
                         {'name': 'phaseId', 'alias': 'phase_id', 'default': 1}]

    q = RabitData(source='api', baseurl=baseurl, uri=uri, route='questionees.do', parameters=params.copy(),
                  index_fields=data_index_fields, json_path='registerJson')

    q.add_filters(field='deleted', condition='EQUAL', value='0')

    md_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='surveys.do', parameters=params)

    fid_path = {'name': 'id', 'default': '0'}
    frm_name_path = {'name': 'surveyName', 'default': ''}
    frm_desc_path = {'name': 'surveyDescription', 'default': ''}

    md_resource = RabitMetadata(source='api', reader=md_reader,
                                fid_path=fid_path, json_path='json', include_html=False,
                                frm_name_path=frm_name_path, frm_desc_path=frm_desc_path)

    md_resource.parse(cache=True, rename_duplicates=False)
    # md_resource.md.to_excel('./sbmu_cancer_registry_metadata.xlsx', index=False)

    data_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='responds.do', parameters=params)

    data_index_fields = [{'name': 'questioneeId', 'alias': 'pid', 'dtype': 'int', 'metadata': {}},
                         {'name': 'surveyId', 'alias': 'frmCode', 'default': 0, 'dtype': 'int'},
                         {'name': 'createdDate', 'alias': 'fillDate'},
                         {'name': 'questionerId', 'alias': 'qid', 'default': 0, 'dtype': 'Int64'},
                         {'name': 'modifiedDate', 'alias': 'last_modified'},
                         {'name': 'phaseId', 'alias': 'phase_id', 'default': 0}
                         ]

    data_resource = RabitData(source='api',
                              reader=data_reader,
                              index_fields=data_index_fields,
                              json_path='respondJson')

    # data = RabitDataset(data=data_resource, metadata=md_resource, project=project_resource)

    # project = get_project_data(baseurl, uri, params)
    # print(project.project_structure)
    #
    # r = []
    # r.append(get_entry_registry_data(baseurl, uri, params))
    #
    # if project.has_paths:
    #     r.append(get_entry_path_data(uri, baseurl, params))
    #
    # r.append(get_response_data(baseurl, uri, params))

    # for resource in r:
    #     resource.load()
    #
    # print(len(r))
