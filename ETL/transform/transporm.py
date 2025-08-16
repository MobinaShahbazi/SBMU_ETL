import json

from ETL.transform.mapping import codes_mapping, title_to_new_codes_mapping
from ETL.extract.extract import survey_respond_records

def coding_mapper(answers: list) -> list:
    """
    Map a list of answers using codes_mapping dict.
    """
    mapped_answer = []
    for answer in answers:
        # Handle missing keys safely
        mapped_answer.append(codes_mapping.get(answer, answer))
    return mapped_answer


def title_to_new_codes_mapper(answer: str) -> list:
    """
    Map a single answer string to new codes.
    """
    try:
        return [title_to_new_codes_mapping.get(answer)]
    except KeyError:
        return [answer]


def record_mapper(survey_respond_records: list) -> list:
    """
    Map records by updating the 'Discharge11' field (if exists) inside respondJson:
    - If the value is a list, map with coding_mapper().
    - If the value is a string, map with title_to_new_codes_mapper().
    Replace key 'Discharge11' with 'FinalDiagnosis'.
    """
    mapped_records = []

    for rec in survey_respond_records:
        rec_copy = rec.copy()  # avoid mutating original record
        respond_json = rec_copy.get("respondJson", {})

        if isinstance(respond_json, str):
            try:
                respond_json = json.loads(respond_json)
            except json.JSONDecodeError:
                respond_json = {}

        if "FinalDiagnosis" in respond_json:
            value = respond_json["FinalDiagnosis"]

            if isinstance(value, list):
                new_value = coding_mapper(value)
            else:
                new_value = value  # fallback if unexpected type

            respond_json["FinalDiagnosis"] = new_value

            rec_copy["respondJson"] = respond_json

        mapped_records.append(rec_copy)

    return mapped_records



if __name__ == '__main__':
    print('before: ')
    x_list = [json.loads(rec['respondJson']) for rec in survey_respond_records]
    print([x.get('FinalDiagnosis') for x in x_list])
    mapped_recoreds = record_mapper(survey_respond_records)
    print('after: ')
    x_list = [rec['respondJson'] for rec in mapped_recoreds]
    print([x.get('FinalDiagnosis') for x in x_list])

