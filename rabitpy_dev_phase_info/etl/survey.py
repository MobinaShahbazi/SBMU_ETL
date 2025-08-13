import json
from .elements import TextElement, RadiogroupElement, CheckboxElement, BooleanElement, Page


class Survey:

    def __init__(self, sid, title, description=None, **kwargs):
        self.id = sid
        self.survey_title = title
        self.survey_description = description
        self.pages = []
        self.current_page_index = None
        self.send_results_on_page_next = kwargs.get('send_results_on_page_next', False)
        self.show_page_titles = kwargs.get('show_page_titles', True)
        self.show_question_numbers = kwargs.get('show_question_numbers', 'on')

    @property
    def json(self):
        return {'pages': [page.to_dict() for page in self.pages],
                'sendResultOnPageNext': self.send_results_on_page_next,
                'showPageTitles': self.show_page_titles,
                'showQuestionNumbers': self.show_question_numbers}

    @property
    def obj(self):

        obj = {
            'id': self.id,
            'json': self.json,
            'surveyName': self.survey_title,
            'surveyDescription': self.survey_description
        }

        return obj

    def insert_page(self, code, title, elements=[]):
        page = Page(code=code, title=title, elements=elements)
        self.pages.append(page)
        self.current_page_index = len(self.pages) - 1

    def insert_el(self, el):
        self.pages[self.current_page_index].elements.append(el)

    def insert_text_el(self, code, title=None, visible_if=None, input_type='text'):
        el = TextElement(code=code, title=title, visible_if=visible_if, input_type=input_type)
        self.insert_el(el=el)

    def insert_boolean_el(self, code, title, visible_if=None, label_true=None, label_false=None):
        el = BooleanElement(code=code, title=title, visible_if=visible_if,
                            label_true=label_true, label_false=label_false)
        self.insert_el(el)

    def insert_radiogroup_el(self, code, title=None, visible_if=None, choices=None):
        el = RadiogroupElement(code=code, title=title, visible_if=visible_if, choices=choices)
        self.insert_el(el)

    def insert_checkbox_el(self, code, title=None, visible_if=None, choices=None):
        el = CheckboxElement(code=code, title=title, visible_if=visible_if, choices=choices)
        self.insert_el(el)

    def update_survey(self, other, position='last'):

        '''
        other: Single RABIT survey type dictionary or a list of RABIT surveys
        position: Position where the survey will be inserter into other
        '''

        if isinstance(other, dict):
            return self.__update_survey(other=other, position=position)
        elif isinstance(other, list):
            for survey in other:
                if survey['id'] == self.id:
                    survey = self.__update_survey(other=survey, position=position)
            return other

        return other

    def __update_survey(self, other, position):

        if position == 'last':
            other['json']['pages'] = other['json']['pages'] + [page.to_dict() for page in self.pages]
        elif position == 'first':
            other['json']['pages'] = [page.to_dict() for page in self.pages]+ other['json']['pages']

        return other

    def to_json(self, survey_json_only=False):

        if survey_json_only:
            return json.dumps(self.json)
        else:
            return json.dumps(self.obj)
