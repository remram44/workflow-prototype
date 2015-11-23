import itertools
from logging import getLogger

from workflow_exec.module import FinishReason


logger = getLogger('interpreter')


class InstanciatedModule(object):
    """A module in the workflow, that consumes inputs and produces outputs.

    This wraps the Module object defined by users.
    """
    _id_gen = itertools.count()

    def __init__(self, interpreter, class_, parameters=None):
        self.instance_id = next(self._id_gen)

        logger.debug("%r created, class=%r", self, class_)

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
        logger.debug("%r starting", self)

        self._instance = self._class(self._parameters, self)
        self._instance.start()
        self._interpreter.started_modules.add(self)

        if not self._expected_input and not self._expect_to_output:
            raise RuntimeError("Module isn't waiting for any event but isn't "
                               "finished (after start())")

    def module_step_unimplemented(self):
        logger.debug("%r doesn't implement step()...", self)

        # If all upstream streams are done, do module_reports_finish()
        for port, stream in self.up.iteritems():
            if stream.producing:
                return

        logger.debug("no step() implementation and upstream streams are done, "
                     "calling finish()")
        self.module_reports_finish()

    def module_requests_input(self, port, nb):
        logger.debug("%r requests input on port %r (%d)", self, port, nb)

        self._expected_input[port] = self._expected_input.get(port, 0) + nb
        self.up[port].wait_input(nb)

    def module_produces_output(self, port, values):
        logger.debug("%r produced output on port %r: %r", self, port, values)

        if port not in self.down:
            return True
        stream = self.down[port]
        ret = stream.push(values)
        if not ret:
            self._expect_to_output = True
            stream.wait_output()
        return ret

    def module_reports_finish(self):
        logger.debug("%r is finished", self)

        for port, stream in self.down.iteritems():
            stream.close()
        self._interpreter.ready_tasks.append(
            FinishTask(self, FinishReason.CALLED_FINISH))

    def finish(self, reason):
        if reason != FinishReason.ALL_OUTPUT_DONE:
            self._interpreter.started_modules.remove(self)
        self._instance.finish(reason)

    def __repr__(self):
        return "<instance %d>" % self.instance_id


class Stream(object):
    """A stream between two ports.

    This is the connection between one producer and multiple consumers. It is
    essentially a buffer.
    """
    _id_gen = itertools.count()

    def __init__(self, interpreter, producer_module, producer_port):
        self.stream_id = next(self._id_gen)

        logger.debug("%r created", self)

        self.interpreter = interpreter
        self.producer_module = producer_module
        self.producer_port = producer_port

        self.waiting = False

        # Whether this stream is still open; True until the producer finishes
        self.producing = True

        # Maps (module, port) to reading position
        self.consumers = set()
        self.position = 0

        self.buffer = []
        self.target_size = 1

    def new_consumer(self, consumer_module, consumer_port):
        endpoint = StreamOutput(self, consumer_module, consumer_port)
        logger.debug("%r: new consumer: %r", self, endpoint)
        self.consumers.add(endpoint)
        return endpoint

    def push(self, values):
        self.buffer.extend(values)

        for endpoint in self.consumers:
            if endpoint.waiting:
                self.interpreter.ready_tasks.append(InputTask(endpoint))

        return len(self.buffer) < self.target_size

    def close(self):
        # TODO
        pass

    def wait_output(self):
        self.waiting = True

    def compact(self):
        # FIXME: optimize
        pos = None
        for endpoint in self.consumers:
            if pos is None:
                pos = endpoint.position
            else:
                pos = min(endpoint.position, pos)
        if pos is None:
            # Discard
            self.position += len(self.buffer)
            self.buffer = []
        else:
            self.buffer = self.buffer[pos - self.position:]
            self.position = pos

    def __repr__(self):
        return "<stream %d>" % self.stream_id


class StreamOutput(object):
    """A downstream endpoint of a stream.

    This keeps some information like the position in the stream.
    """
    def __init__(self, stream, consumer_module, consumer_port):
        self.stream = stream
        self.position = 0
        self.consumer_module = consumer_module
        self.consumer_port = consumer_port
        self.requested = 0
        self.waiting = False

    def wait_input(self, nb):
        # The consumer requests input
        # If we have it, queue a task immediately; else mark the endpoint
        available = self.stream.position + len(self.stream.buffer)
        if available - self.position:
            self.stream.interpreter.ready_tasks.append(InputTask(self))
        if not self.requested:
            self.waiting = True
        self.requested = nb

    def __repr__(self):
        return "<endpoint of stream %d: %r, port %r>" % (
            self.stream.stream_id, self.consumer_module, self.consumer_port)


class Task(object):
    """A scheduled interpreter task, that will eventually be executed.
    """
    def execute(self):
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

    def execute(self):
        self._module.start()


class InputTask(Task):
    """Feed more input to a module that requested it.
    """
    def __init__(self, stream_output):
        """
        :type stream_output: StreamOutput
        """
        Task.__init__(self)
        self._stream_output = stream_output

    def execute(self):
        endpoint = self._stream_output
        stream = endpoint.stream
        module = endpoint.consumer_module
        port = endpoint.consumer_port

        module._instance.input_list(
            port,
            stream.buffer[endpoint.position - stream.position:])
        endpoint.position = stream.position + len(stream.buffer)

        stream.compact()


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

    def execute(self):
        self._module.finish(self._reason)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        # Make fake pipeline
        # TODO: every port here is treated as depth=1 ports
        def connect(umod, uport, dmod, dport):
            assert dport not in dmod.up
            if uport not in umod.down:
                stream = umod.down[uport] = Stream(self, umod, uport)
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

                task.execute()

                #for dep in task.dependents:
                #    dep.dependencies.remove(dep)
                #    if not dep.dependencies:
                #        self.ready_tasks.append(dep)

            if self.dependent_tasks:
                raise RuntimeError("There are still tasks but nothing can be "
                                   "executed (deadlock)")

            assert not self.started_modules
        finally:
            for module in list(self.started_modules):
                module.finish(FinishReason.TERMINATE)
