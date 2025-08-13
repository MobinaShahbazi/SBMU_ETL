import re


class RabitExpression:

    def __init__(self, expression: str):
        self.er = expression
        self._args = None
        self._e = None

    @property
    def args(self):
        return set(re.findall(r'{(\w+)}', self.er))

    @property
    def nargs(self):
        return len(self.args)

    @property
    def e(self):
        e = self.er
        for i, arg in enumerate(self.args):
            pattern = r'\{' + arg + r'\}'
            e = re.sub(pattern, f"arg{i}", e)
        return e

    def as_pd_expression(self, name):
        e = self.er
        for i, arg in enumerate(self.args):
            pattern = r'\{' + arg + r'\}'
            e = re.sub(pattern, f"{name}['{arg}']",  e)
        return e
