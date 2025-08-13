from dataclasses import dataclass, field
from typing import Union


class Page:

    def __init__(self, code, title, elements=[]):
        self.code = code
        self.title = title
        self.elements = elements

    @property
    def obj(self):
        obj = {'name': self.code, 'title': self.title, 'elements': self.elements}
        return obj

    def to_dict(self):
        return {'name': self.code, 'title': self.title, 'elements': [el.to_dict() for el in self.elements]}


class ElementBaseClass:

    def __init__(self, el_type, code, title, is_required, visible_if):
        self.el_type = el_type
        self.code = code
        self.title = title
        self.visible_if = visible_if
        self.is_required = is_required

    @property
    def obj(self):
        obj = {
            'type': self.el_type,
            'name': self.code,
            'title': self.title,
            'visibleIf': self.visible_if,
            'isRequired': self.is_required
        }
        return obj

    def to_dict(self):
        return {k: v for k, v in self.obj.items() if v is not None}


class Choices(ElementBaseClass):

    def __init__(self, choices=None):
        self.choices = choices

    @property
    def choices(self):
        return self._choices

    @choices.setter
    def choices(self, c_iter=None):

        if isinstance(c_iter, dict):
            self._choices = [{'value': k, 'text': v} if v else k for k, v in c_iter.items()]
        elif c_iter is None:
            self._choices = None

    @property
    def obj(self):
        return self.choices


class TextElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False, input_type='text', new_line=True):

        super().__init__(el_type='text', code=code, title=title, visible_if=visible_if, is_required=is_required)
        self.legal_types = ['text', 'number', 'date', 'jalalidate']
        self.new_line = new_line
        self._input_type = input_type

    @property
    def input_type(self):
        return self._input_type

    @input_type.setter
    def input_type(self, value):
        if value in self.legal_types:
            self._input_type = value
        else:
            raise ValueError(f'Unsupported input_type {value}')

    @property
    def obj(self):

        obj = super().obj
        obj.update({'inputType': self.input_type, 'startWithNewLine': self.new_line})

        return obj


class RadiogroupElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False, choices=None, col_count=1):

        super().__init__(el_type='radiogroup', code=code, title=title, visible_if=visible_if, is_required=is_required)

        self.choices = Choices(choices=choices)
        self.col_count = col_count

    @property
    def obj(self):
        obj = super().obj
        obj.update({'choices': self.choices.obj, 'colCount': self.col_count})
        return obj


class CheckboxElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False, choices=None):
        super().__init__(el_type='checkbox', code=code, title=title, visible_if=visible_if, is_required=is_required)
        self.choices = Choices(choices=choices)

    @property
    def obj(self):
        obj = super().obj
        obj.update({'choices': self.choices.obj})
        return obj


class BooleanElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False, label_true=None, label_false=None):

        super().__init__(el_type='boolean', code=code, title=title, visible_if=visible_if, is_required=is_required)
        self.label_true = label_true
        self.label_false = label_false

    @property
    def obj(self):
        obj = super().obj
        obj.update({'labelTrue': self.label_true, 'labelFalse': self.label_false})
        return obj


class MatrixElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False, columns=[], rows=[]):
        super().__init__(el_type='matrix', code=code, title=title, visible_if=visible_if, is_required=is_required)
        self.columns = columns
        self.rows = rows

    @property
    def obj(self):
        obj = super().obj
        obj.update({'columns': self.columns, 'rows': self.rows})

        return obj


class PanelElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, elements=None, is_required=False):
        super().__init__(el_type='panel', code=code, title=title, visible_if=visible_if, is_required=is_required)
        self.elements = elements

    @property
    def obj(self):
        obj = super().obj
        obj.update({'elements': self.elements})

        return obj


class HTMLElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, html=''):
        super().__init__(el_type='html', code=code, title=title, visible_if=visible_if, is_required=None)
        self.html = html

    @property
    def obj(self):
        obj = super().obj
        obj.update({'html': self.html})

        return obj


class CommentElement(ElementBaseClass):

    def __init__(self, code, title=None, visible_if=None, is_required=False):
        super().__init__(el_type='comment', code=code, title=title, visible_if=visible_if, is_required=is_required)

