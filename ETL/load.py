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



