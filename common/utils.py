from ETL.transform.mapping import codes_mapping, title_to_new_codes_mapping


def coding_mapper(answers: list) -> list:
    mapped_answer = []
    for answer in answers:
        mapped_answer.append(codes_mapping[answer])

    return mapped_answer

def title_to_new_codes_mapper(answer: str) -> list:
    mapped_answer = [title_to_new_codes_mapper(answer)]
    return mapped_answer