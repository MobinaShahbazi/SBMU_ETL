# from common.utils import get_project_data, get_entry_registry_data, get_response_data, get_entry_path_data
#
# if __name__ == '__main__':
#
#     source = 'api'
#     # baseurl = 'https://panel.rabit.ir'
#     # uri = '6fabec64-081f-4b40-834a-f3a94b348a73'
#     baseurl = 'https://disreg.sbmu.ac.ir'
#     uri = '464b3f1a-73d1-460b-af45-f628e01571f7'
#     params = {'unpaged': 1}
#
#     project = get_project_data(baseurl, uri, params)
#     print(project.project_structure)
#
#     r = []
#     r.append(get_entry_registry_data(baseurl, uri, params))
#
#     if project.has_paths:
#         r.append(get_entry_path_data(uri, baseurl, params))
#
#     r.append(get_response_data(baseurl, uri, params))
#
#     for resource in r:
#         resource.load()
#
#     print(len(r))
#     # response_data = get_response_data(baseurl, uri, {})
#     # print(type(response_data))
#
