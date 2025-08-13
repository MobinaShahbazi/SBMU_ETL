import json
import requests
from typing import Any, Dict, Optional

def save_response(
    host: str,
    uri: str,
    project_id: int,
    respond_json: Dict[str, Any],
    survey_id: int,
    questioner_id: int,
    questionee_id: int,
    questionee_guid: Optional[str] = None,
    id: Optional[int] = None
) -> requests.Response:
    """
    Sends a POST request to save a survey response.

    Args:
        host (str): The base URL of the API.
        uri (str): The endpoint URI.
        project_id (int): The project identifier.
        respond_json (Dict[str, Any]): The response data in JSON format.
        survey_id (int): The survey identifier.
        questioner_id (int): The ID of the person asking questions.
        questionee_id (int): The ID of the person answering questions.
        questionee_guid (Optional[str], optional): Unique identifier for the questionee. Defaults to None.
        id (Optional[int], optional): Response ID. Defaults to None.

    Returns:
        requests.Response: The response object from the POST request.
    """
    baseurl = f'{host}/api/{uri}/save-respond'
    body = {
        'id': id,
        'projectId': project_id,
        'respondJson': json.dumps(respond_json, ensure_ascii=False),
        'surveyId': survey_id,
        'questionerId': questioner_id,
        'questioneeId': questionee_id,
        'questioneeGuid': questionee_guid
    }
    return requests.post(url=baseurl, json=body, verify=False)



# from rabitpy_dev_phase_info.io.resources import RabitProject, RabitMetadata, RabitData, RabitDataset
# from rabitpy_dev_phase_info.io.adapters import RabitReaderAPIAdapter
#
#
# def get_project_data(baseurl, uri, params):
#
#     reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='project.do', parameters=params)
#     pr = RabitProject(source='api', reader=reader)
#     pr.parse(cache=True)
#
#     # Extract Project Paths
#     paths_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='paths.do', parameters={'pagesize': 1})
#     paths = paths_reader.fetch()
#
#     pr.has_paths = len(paths) >= 1
#
#     return pr
#
# def get_entry_registry_data(baseurl, uri, params):
#     # Extract Project Paths
#     reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='project.do', parameters=params)
#
#     fid_path = {'name': 'id', 'default': '0'}
#     frm_name_path = {'name': 'title', 'default': 'registry'}
#     frm_desc_path = {'name': 'description', 'default': ''}
#
#     metadata = RabitMetadata(source='api', reader=reader,
#                              fid_path=fid_path, json_path='registryQuestioneeSurveyJson',
#                              include_html=False, frm_name_path=frm_name_path, frm_desc_path=frm_desc_path)
#
#     reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='questionees.do', parameters=params)
#     reader.add_filter(field='deleted', condition='EQUAL', value='0')
#
#     entry_index_fields = [{'name': 'id', 'alias': 'pid', 'dtype': 'int'},
#                           {'name': '_survey_id', 'alias': 'frmCode', 'default': 0, 'dtype': 'int'},
#                           {'name': 'createdDate', 'alias': 'fillDate'},
#                           {'name': 'createdBy', 'alias': 'qid', 'default': 0, 'dtype': 'Int64'},
#                           {'name': 'modifiedDate', 'alias': 'last_modified'},
#                           {'name': 'phaseId', 'alias': 'phase_id', 'default': 0, 'dtype': 'Int64'}]
#
#     data = RabitData(source='api',
#                      reader=reader,
#                      index_fields=entry_index_fields,
#                      json_path='registerJson')
#
#     entry = RabitDataset(data=data, metadata=metadata)
#
#     return entry
#
# def get_entry_path_data(baseurl, uri, params):
#
#     # Extract Project Paths
#     paths_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='paths.do', parameters=params)
#
#     fid_path = {'name': 'id', 'default': '0'}
#     frm_name_path = {'name': 'title', 'default': ''}
#     frm_desc_path = {'name': 'description', 'default': ''}
#
#     paths_resource = RabitMetadata(source='api', reader=paths_reader,
#                                    fid_path=fid_path, json_path='surveyJson',
#                                    include_html=False, frm_name_path=frm_name_path, frm_desc_path=frm_desc_path)
#
#     entry_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='questionees.do', parameters=params)
#     entry_reader.add_filter(field='deleted', condition='EQUAL', value='0')
#
#     entry_index_fields = [{'name': 'id', 'alias': 'pid', 'dtype': 'int', 'metadata': {}},
#                           {'name': 'pathId', 'alias': 'frmCode', 'default': 0, 'dtype': 'int'},
#                           {'name': 'createdDate', 'alias': 'fillDate'},
#                           {'name': 'createdBy', 'alias': 'qid', 'default': 0, 'dtype': 'Int64'},
#                           {'name': 'modifiedDate', 'alias': 'last_modified'},
#                           {'name': 'phaseId', 'alias': 'phase_id', 'default': 0, 'dtype': 'Int64'}]
#
#     entry_resource = RabitData(source='api',
#                                reader=entry_reader,
#                                index_fields=entry_index_fields,
#                                json_path='pathRespond')
#
#     entry = RabitDataset(data=entry_resource, metadata=paths_resource)
#     return entry
#
# def get_response_data(baseurl, uri, params):
#
#     md_resource = RabitMetadata(source='api',
#                                 reader=RabitReaderAPIAdapter(baseurl=baseurl,
#                                                              uri=uri,
#                                                              route='surveys.do',
#                                                              parameters=params),
#                                 fid_path={'name': 'id', 'default': '95'},
#                                 json_path='json',
#                                 include_html=False,
#                                 frm_name_path={'name': 'surveyName', 'default': ''},
#                                 frm_desc_path={'name': 'surveyDescription', 'default': ''})
#
#     md_resource.parse(cache=True, rename_duplicates=False)
#
#     data_reader = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='responds.do', parameters=params)
#
#     data_index_fields = [{'name': 'questioneeId', 'alias': 'pid', 'dtype': 'int'},
#                          {'name': 'surveyId', 'alias': 'frmCode', 'default': 0, 'dtype': 'int'},
#                          {'name': 'createdDate', 'alias': 'fillDate'},
#                          {'name': 'questionerId', 'alias': 'qid', 'default': 0, 'dtype': 'Int64'},
#                          {'name': 'modifiedDate', 'alias': 'last_modified'},
#                          {'name': 'phaseId', 'alias': 'phase_id', 'default': 0}
#                          ]
#
#     data_resource = RabitData(source='api',
#                               reader=data_reader,
#                               index_fields=data_index_fields,
#                               json_path='respondJson')
#
#     data = RabitDataset(data=data_resource, metadata=md_resource)
#     return data