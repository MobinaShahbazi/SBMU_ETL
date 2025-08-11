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