import itertools
from logging import getLogger

from workflow_exec.module import FinishReason


logger = getLogger('interpreter')


class InstantiatedModule(object):
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
        self._finished = False

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
        for port, endpoint in self.up.iteritems():
            if endpoint.stream.producing:
                return

        logger.debug("no step() implementation and upstream streams are done, "
                     "calling finish()")
        self.module_reports_finish()

    def module_requests_input(self, port, all_available):
        if all_available:
            logger.debug("%r requests all available input on port %r",
                         self, port)
        else:
            logger.debug("%r requests input on port %r",
                         self, port)

        self._expected_input[port] = all_available
        self.up[port].wait_input()

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
        logger.debug("%r reports finish", self)

        for port, stream in self.down.iteritems():
            stream.close()
        self._interpreter.task_queue.append(
            FinishTask(self, FinishReason.CALLED_FINISH))

    def finish(self, reason):
        if self._finished:
            return

        logger.debug("%r finished, reason=%s", self, reason)

        if reason != FinishReason.ALL_OUTPUT_DONE:
            logger.debug("%r removed", self)
            self._interpreter.started_modules.remove(self)
            self._finished = True
        self._instance.finish(reason)

    def stream_end(self, port):
        logger.debug("stream finished: %r, port %r", self, port)
        self._instance.input_end(port)

        # If all upstream streams are done, do finish(ALL_OUTPUT_DONE)
        for port, endpoint in self.up.iteritems():
            if endpoint.stream.producing:
                return

        logger.debug("all input done")
        self._instance.all_input_end()

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
                endpoint.waiting = False
                self.interpreter.task_queue.append(InputTask(endpoint))

        return len(self.buffer) < self.target_size

    def close(self):
        logger.debug("Closing %r", self)
        self.producing = False
        for endpoint in self.consumers:
            endpoint.consumer_module.stream_end(endpoint.consumer_port)

    def wait_output(self):
        if not self.waiting:
            logger.debug("%r.waiting = True", self)
        self.waiting = True

    def compact(self):
        # FIXME: optimize
        pos = None
        for endpoint in self.consumers:
            if pos is None:
                pos = endpoint.position
            else:
                pos = min(endpoint.position, pos)
        old_pos, old_len = self.position, len(self.buffer)
        if pos is None:
            # Discard
            self.position += len(self.buffer)
            self.buffer = []
        else:
            self.buffer = self.buffer[pos - self.position:]
            self.position = pos

        logger.debug("Compacted %r: %d:%d -> %d:%d",
                     self, old_pos, old_len, self.position, len(self.buffer))

        if self.waiting:
            self.waiting = False
            self.interpreter.task_queue.append(
                OutputTask(self.producer_module))
            logger.debug("%r.waiting = False & adding OutputTask", self)

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
        self.requested = False
        self.waiting = False

    def wait_input(self):
        # The consumer requests input
        # If we have it, queue a task immediately; else mark the endpoint
        available = self.stream.position + len(self.stream.buffer)
        logger.debug("requested input is %s; "
                     "endpoint: %r, stream: %d-%d",
                     "available" if available - self.position > 0
                     else "not available",
                     self.position, self.stream.position, available)
        if available - self.position > 0:
            self.stream.interpreter.task_queue.append(InputTask(self))
        if not self.requested:
            logger.debug("%r.waiting = True", self)
            self.waiting = True
        self.requested = True

        if self.stream.producer_module._instance is None:
            logger.debug("%r not yet instantiated, adding StartTask",
                         self.stream.producer_module)
            self.stream.interpreter.task_queue.append(
                StartTask(self.stream.producer_module))

    def __repr__(self):
        return "<endpoint of stream %d: %r, port %r>" % (
            self.stream.stream_id, self.consumer_module, self.consumer_port)


class Task(object):
    """A scheduled interpreter task, that will eventually be executed.
    """
    def execute(self):
        raise NotImplementedError


class StartTask(Task):
    """Instantiate and start executing a module.
    """
    def __init__(self, module):
        """
        :type module: InstantiatedModule
        """
        Task.__init__(self)
        self._module = module

    def execute(self):
        if self._module._instance is None:
            self._module.start()

    def __repr__(self):
        return "StartTask(module=%r)" % self._module


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

        logger.debug("%r.waiting = False", self)
        endpoint.waiting = False
        endpoint.requested = False
        all_available = module._expected_input.pop(port)
        logger.debug("endpoint: %d, stream: %d-%d",
                     endpoint.position,
                     stream.position, stream.position + len(stream.buffer))
        if all_available:
            logger.debug("Feeding all input to %r, port %r: "
                         "%d elements, %d-%d",
                         module, port,
                         stream.position + len(buffer) - endpoint.position,
                         endpoint.position,
                         stream.position + len(stream.buffer))
            feed = stream.buffer[endpoint.position - stream.position:]
            endpoint.position = stream.position + len(stream.buffer)
            module._instance.input_list(
                port,
                feed)
        else:
            logger.debug("Feeding one input to %r, port %r: %d",
                         module, port, endpoint.position)
            feed = [stream.buffer[endpoint.position - stream.position]]
            endpoint.position += 1
            module._instance.input_list(
                port,
                feed)

        stream.compact()

    def __repr__(self):
        return "InputTask(endpoint=%r)" % self._stream_output


class OutputTask(Task):
    """Allow a module to produce more output by calling step().
    """
    def __init__(self, module):
        """
        :type module: InstantiatedModule
        """
        self._module = module

    def execute(self):
        self._module._instance.step()

    def __repr__(self):
        return "OutputTask(module=%r)" % self._module


class FinishTask(Task):
    """Call finish() on a module.
    """
    def __init__(self, module, reason):
        """
        :type module: InstantiatedModule
        """
        Task.__init__(self)
        self._module = module
        self._reason = reason

    def execute(self):
        self._module.finish(self._reason)

    def __repr__(self):
        return "FinishTask(module=%r, %r)" % (self._module, self._reason)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        # ####################
        # + FAKE PIPELINE
        # TODO: every port here is treated as depth=1 ports
        def connect(umod, uport, dmod, dport):
            assert dport not in dmod.up
            if uport not in umod.down:
                stream = umod.down[uport] = Stream(self, umod, uport)
            else:
                stream = umod.down[uport]
            dmod.up[dport] = stream.new_consumer(dmod, dport)

        m0 = InstantiatedModule(self, basic.Constant,
                                {'value': '/etc/resolv.conf'})
        m1 = InstantiatedModule(self, basic.ReadFile)
        connect(m0, 'value', m1, 'path')
        m2 = InstantiatedModule(self, basic.Count)
        connect(m1, 'line', m2, 'data')
        m3 = InstantiatedModule(self, basic.RandomNumbers)
        m4 = InstantiatedModule(self, basic.Zip)
        connect(m1, 'line', m4, 'left')
        connect(m3, 'number', m4, 'right')

        m5 = InstantiatedModule(self, basic.StandardOutput)
        connect(m2, 'length', m5, 'data')
        m6 = InstantiatedModule(self, basic.StandardOutput)
        connect(m4, 'zip', m6, 'data')

        sinks = [m5, m6]
        logger.debug("Fake pipeline created")
        # - FAKE PIPELINE
        # ####################

        self.started_modules = set()
        self.task_queue = [StartTask(mod) for mod in sinks]

        try:
            while self.task_queue:
                logger.debug("========================================")
                logger.debug("Tasks:\n%s",
                             '\n'.join("    %r" % t for t in self.task_queue))
                logger.debug("Started modules: %s",
                             ' '.join('%d' % m.instance_id
                                      for m in self.started_modules))

                task = self.task_queue.pop(0)

                task.execute()

            #assert not self.started_modules
        finally:
            logger.debug("Execution done, killing remaining modules: %s",
                         ' '.join('%d' % m.instance_id
                                  for m in self.started_modules))
            for module in list(self.started_modules):
                module.finish(FinishReason.TERMINATE)
