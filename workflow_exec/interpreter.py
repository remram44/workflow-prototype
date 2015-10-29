class InstanciatedModule(object):
    def __init__(self, class_, parameters=None):
        if parameters is None:
            parameters = {}
        self._instance = class_(parameters, self)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        a = InstanciatedModule(basic.Constant)
