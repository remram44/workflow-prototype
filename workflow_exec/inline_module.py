from workflow_exec.module import Module, FinishReason as _FinishReason


class EndOfInput(Exception):
    """A get_input() call fails because there is no more input to be read.
    """
    def __init__(self, ports):
        self.ports = ports


class FinishExecution(Exception):
    """Module execution is finishing early.

    This corresponds to the Module#finish() event. There is no exception for
    `CALLED_FINISH`, since that corresponds to calling ``return`` in the
    generator.
    """


class ExecutionTerminated(FinishExecution):
    """Module should finish up because of error or execution aborted.

    The module should finish quickly. Any more output will likely get ignored.
    """


class AllOutputDone(FinishExecution):
    """The output streams have no more readers.

    This module can finish executing now since no one is reading its output
    anymore.
    """


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
        # requested_ports, missing_ports, values
        self._input_request = None
        self._step(return_=None)

    def _step(self, **kwargs):
        raise_ = False
        assert len(kwargs) == 1
        k, arg = next(kwargs.items())
        if k == 'raise_':
            raise_ = True
        elif k != 'return_':
            raise TypeError("_step() got unexpected kwargs %r" % k)

        while True:
            try:
                if raise_:
                    req = self._gen.throw(arg)
                else:
                    req = self._gen.send(arg)
            except (StopIteration, FinishExecution):
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

    def input(self, port, value):
        # Add this to the ports the module is waiting on
        self._input_request[1].remove(port)
        self._input_request[2][port] = value

        # If we have everything to resume, do it
        if not self._input_request[1]:
            values = []
            for port_name in self._input_request[0]:
                values.append(self._input_request[2][port_name])
            self._input_request = None
            self._step(return_=values)

    def input_end(self, port):
        # No need to save the port that ended, the interpreter will signal it
        # again if we ask for input on a finished port
        if port in self._input_request[1]:
            self._step(raise_=EndOfInput((port,)))

    def all_input_end(self):
        if self._input_request is not None:
            self._step(raise_=EndOfInput(tuple(self._input_request[0])))

    def step(self):
        if self._input_request is None:
            self._step(return_=None)

    def finish(self, reason):
        if reason == _FinishReason.TERMINATE:
            self._step(raise_=ExecutionTerminated)
        elif reason == _FinishReason.ALL_OUTPUT_DONE:
            self._step(raise_=AllOutputDone)
