from workflow_exec.module import FinishReason


class InstanciatedModule(object):
    """A module in the workflow, that consumes inputs and produces outputs.

    This wraps the Module object defined by users.
    """
    def __init__(self, interpreter, class_, parameters=None):
        self._interpreter = interpreter
        if parameters is None:
            self._parameters = {}
        else:
            self._parameters = parameters
        self.up = {}
        self.down = {}
        self._class = class_
        self._instance = None

        self._expected_input = {}
        self._expect_to_output = False

    def start(self):
        self._instance = self._class(self._parameters, self)
        self._instance.start()
        self._interpreter.started_modules.add(self)

        if not self._expected_input and not self._expect_to_output:
            raise RuntimeError("Module isn't waiting for any event but isn't "
                               "finished (after start())")

    def module_step_unimplemented(self):
        # If all upstream streams are done, do module_reports_finish()
        for port, stream in self.up.iteritems():
            if stream.producing:
                return

        self.module_reports_finish()

    def module_requests_input(self, port, nb):
        self._expected_input[port] = self._expected_input.get(port, 0) + nb
        # TODO: call input() sometime

    def module_produces_output(self, port, values):
        if port not in self.down:
            return True
        stream = self.down[port]
        ret = stream.push(values)
        if not ret:
            self._expect_to_output = True
            # TODO: call step() sometime
            pass
        return ret

    def module_reports_finish(self):
        for port, stream in self.down.iteritems():
            stream.close()
        self._interpreter.ready_tasks.append(
            FinishTask(self, FinishReason.CALLED_FINISH))

    def finish(self, reason):
        if reason != FinishReason.ALL_OUTPUT_DONE:
            self._interpreter.started_modules.remove(self)
        self._instance.finish(reason)


class Stream(object):
    """A stream between two ports.

    This is the connection between one producer and multiple consumers. It is
    essentially a buffer.
    """
    def __init__(self, producer_module, producer_port):
        self.producer_module = producer_module
        self.producer_port = producer_port

        # Whether this stream is still open; True until the producer finishes
        self.producing = True

        # Maps (module, port) to reading position
        self.consumers = set()
        self.position = 0

        self.buffer = []
        self.target_size = 1

    def new_consumer(self, consumer_module, consumer_port):
        endpoint = StreamOutput(self, consumer_module, consumer_port)
        self.consumers.add(endpoint)
        return endpoint

    def push(self, values):
        self.buffer.extend(values)
        return len(self.buffer) < self.target_size

    def close(self):
        # TODO
        pass


class StreamOutput(object):
    """A downstream endpoint of a stream.

    This keeps some information like the position in the stream.
    """
    def __init__(self, stream, consumer_module, consumer_port):
        self.stream = stream
        self.position = 0
        self.consumer_module = consumer_module
        self.consumer_port = consumer_port


class Task(object):
    """A scheduled interpreter task, that will eventually be executed.
    """
    def __init__(self):
        self.dependents = set()
        self.dependencies = set()

    def execute(self, interpreter):
        raise NotImplementedError


class StartTask(Task):
    """Instanciate and start executing a module.
    """
    def __init__(self, module):
        """
        :type module: InstanciatedModule
        """
        Task.__init__(self)
        self._module = module

    def execute(self, interpreter):
        self._module.start()


class FinishTask(Task):
    """Call finish() on a module.
    """
    def __init__(self, module, reason):
        """
        :type module: InstanciatedModule
        """
        Task.__init__(self)
        self._module = module
        self._reason = reason

    def execute(self, interpreter):
        self._module.finish(self._reason)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        # Make fake pipeline
        # TODO: every port here is treated as depth=1 ports
        def connect(umod, uport, dmod, dport):
            assert dport not in dmod.up
            if uport not in umod.down:
                stream = umod.down[uport] = Stream(umod, uport)
            else:
                stream = umod.down[uport]
            dmod.up[dport] = stream.new_consumer(dmod, dport)

        a = InstanciatedModule(self, basic.Constant, {'value': '/etc/passwd'})
        b = InstanciatedModule(self, basic.ReadFile)
        connect(a, 'value', b, 'path')
        c = InstanciatedModule(self, basic.RandomNumbers)
        d = InstanciatedModule(self, basic.ParitySplitter)
        connect(c, 'number', d, 'number')
        e = InstanciatedModule(self, basic.Zip)
        connect(b, 'line', e, 'left')
        connect(d, 'odd', e, 'right')
        f = InstanciatedModule(self, basic.Sample, {'rate': 5})
        connect(e, 'zip', f, 'data')
        g = InstanciatedModule(self, basic.Sample, {'rate': 20})
        connect(d, 'even', g, 'data')
        h = InstanciatedModule(self, basic.AddPrevious)
        connect(g, 'sampled', h, 'number')
        i = InstanciatedModule(self, basic.Count)
        connect(h, 'sum', i, 'data')
        j = InstanciatedModule(self, basic.Format,
                               {'format': "right branch ({0})"})
        connect(i, 'length', j, 'element')
        k = InstanciatedModule(self, basic.Format, {'format': "sum: {0}"})
        connect(h, 'sum', k, 'element')
        l = InstanciatedModule(self, basic.StandardOutput)
        connect(k, 'string', l, 'data')
        m = InstanciatedModule(self, basic.StandardOutput)
        connect(f, 'sampled', m, 'data')
        n = InstanciatedModule(self, basic.StandardOutput)
        connect(j, 'string', n, 'data')

        sinks = [m, n, l]

        self.started_modules = set()
        self.ready_tasks = [StartTask(mod) for mod in sinks]
        self.dependent_tasks = set()

        try:
            while self.ready_tasks:
                task = self.ready_tasks.pop(0)

                task.execute(self)

                for dep in task.dependents:
                    dep.dependencies.remove(dep)
                    if not dep.dependencies:
                        self.ready_tasks.append(dep)

            if self.dependent_tasks:
                raise RuntimeError("There are still tasks but nothing can be "
                                   "executed (deadlock)")

            assert not self.started_modules
        finally:
            for module in list(self.started_modules):
                module.finish(FinishReason.TERMINATE)
