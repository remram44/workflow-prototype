class Module(object):
    def __init__(self, parameters, interpreter):
        self.parameters = parameters
        self._interpreter = interpreter

    def start(self):
        pass

    def input(self, port, value):
        raise KeyError("Unexpected input %r" % port)

    def input_end(self, port):
        pass

    def step(self):
        pass

    def finish(self, reason):
        pass

    def _request_input(self, port, nb=1):
        todo

    def _output(self, port, value):
        todo
        return True

    def _finish(self):
        todo
