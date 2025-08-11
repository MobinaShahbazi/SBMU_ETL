from mapping import mapping_dict


def coding_mapper(answers: list):
    mapped_answer = []
    for answer in answers:
        mapped_answer.append(mapping_dict[answer])

    return mapped_answer


print(coding_mapper(['I01','I05']))