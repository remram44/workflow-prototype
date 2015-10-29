class Module(object):
    def __init__(self, parameters, interface):
        self.parameters = parameters
        self._interface = interface

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
        self._interface.module_requests_input(port, nb)

    def _output(self, port, value):
        return self._interface.module_produces_output(port, value)

    def _finish(self):
        self._interface.module_reports_finish()
