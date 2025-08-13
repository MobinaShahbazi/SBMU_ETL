from ETL.transform.mapping import codes_mapping, title_to_new_codes_mapping


def coding_mapper(answers: list) -> list:
    mapped_answer = []
    for answer in answers:
        mapped_answer.append(codes_mapping[answer])

    return mapped_answer

def title_to_new_codes_mapper(answer: str) -> list:
    mapped_answer = [title_to_new_codes_mapping(answer)]
    return mapped_answer

def record_mapper(survey_respond_records: list) -> list:
    mapped_records = []
    for rec in survey_respond_records:
        """ 
        i want mapped_records to have all fields of all records in survey_respond_records but i need to update one field.
        each rec ic a dict and in each rec there is a field named respondJson which is a dict. if respondJson has a field named Discharge11, first i have to check
        the value type, if it is a list i have to update it like this: new_value = coding_mapper(respondJson["Discharge11"]). but if the type was str then
        i have to update it like this: new_value = title_to_new_codes_mapper(respondJson["Discharge11"]). take caution that Discharge11 may not be in the resopndJson. 
        also  maybe keys doesnot exist in dictionaries i used in title_to_new_codes_mapping or coding_mapper. handel this and complete it.
        changeeeeeeeeeeeeeeee  Discharge11 -> FinalDiagnosis
        
        """

        pass



if __name__ == '__main__':
    # check type str or list for answer
    # Discharge11 -> FinalDiagnosis
    pass