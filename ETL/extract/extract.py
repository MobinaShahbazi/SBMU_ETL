from config import *
from rabitpy_dev_phase_info.io.adapters import RabitReaderAPIAdapter

from rabitpy.io.rdata import RabitResource

def extract_survey_respond_disreg(baseurl: str, uri: str, params:dict) -> list:
    """
    Fetch survey respond records from the API and filter
    """
    # to get questionees instead of responds -> same function, but we use route='questionees.do'
    request = RabitResource(
        kind='data',
        source='api',
        baseurl=baseurl,
        uri=uri,
        route='responds.do',
        parameters=params,
        pid_path='id',
        fill_date_path='createdDate',
        usefields='all',
        headers={'Authorization': 'Basic UkBiaVQwMTpWaXNUQFJAYml0'}
    )
    survey_respond_records = request.fetch()['content']

    return survey_respond_records


def extract_survey_respond_panel(baseurl: str, uri: str, params: dict) -> list:
    """
    Fetch and return all survey respond records from the API.
    """
    # to get questionees instead of responds -> same function, but we use route='questionees.do'
    request = RabitReaderAPIAdapter(baseurl=baseurl, uri=uri, route='responds.do', parameters=params)
    survey_respond_records = request.fetch()['content']
    return survey_respond_records


if __name__ == '__main__':

    survey_respond_records = extract_survey_respond_disreg(BASEURL_DISREG, URI_DISREG, PARAMS)
    filtered_records = [rec for rec in survey_respond_records if rec.get('surveyId') == 84645][:3]
    print(filtered_records)

    # /////////////

    survey_respond_records = extract_survey_respond_panel(BASEURL_PANEL, URI_PANEL, PARAMS)
    filtered_records = [rec for rec in survey_respond_records if rec.get('surveyId') == 167001]
    print(filtered_records)
