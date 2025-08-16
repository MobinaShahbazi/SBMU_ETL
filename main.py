from ETL.load import save_response
from ETL.transform.transporm import mapped_recoreds
from config import *

if __name__ == '__main__':

    print('Start loading...')
    for rec in mapped_recoreds:
        try:
            response = save_response(
                host=BASEURL_PANEL, uri=URI_PANEL, project_id=project_id_PANEL,
                survey_id=SURVEY_ID_PANEL, questionee_id=414382,
                questioner_id=34794, respond_json=rec['respondJson'],
            )
            print(response.status_code)
            print(f'Saved {rec}')

        except Exception as e:
            print(f'Failed to save form: {e}')