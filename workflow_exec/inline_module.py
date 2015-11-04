from workflow_exec.module import Module


class EndOfInput(Exception):
    """A get_input() call fails because there is no more input to be read.
    """
    def __init__(self, ports):
        self.ports = ports


def inline_module(func):
    return type(func.func_name, (InlineModule,), {'_func': func})


class ReqInput(tuple):
    pass


class ReqOutput(tuple):
    pass


class InlineModuleInterface(object):
    def __init__(self, module):
        self.__module = module

    def get_input(self, *ports):
        return ReqInput(ports)

    def output(self, port, value):
        return ReqOutput((port, value))


class InlineModule(Module):
    def start(self):
        self._gen = self._func(InlineModuleInterface(self),
                               self.parameters)
        self._step(None)

    def _step(self, arg):
        while True:
            try:
                req = self._gen.send(arg)
            except StopIteration:
                # Generator returned -- module is done
                self._finish()
                return

            if isinstance(req, ReqInput):
                self._input_request = req, set(req), {}
                for port in req:
                    self._request_input(port)
                return
            elif isinstance(req, ReqOutput):
                port, value = req
                if self._output(port, value):
                    continue
                else:
                    return

    # WIP
