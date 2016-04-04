"""Pipeline scheduling and execution logic.

This is the actual interpreter, creating and executing Module subclasses as
data flows.

Basically driven by the Stream objects: the interpreter has a list of tasks
that it executes in order; when a stream gets full, a task to resume production
once it isn't gets created; when a stream gets empty, a task to produce more
data on it gets created.

TODO: tasks should have enough constraints to allow multiprocessing through
task-stealing, allowing full parallel execution.

InstantiatedModule wraps a Module, keeping track of current state, requested
inputs, input and output streams' statuses.

Stream is a single-producer multiple-consumer queue that connects module ports,
creating tasks with the Interpreter when needed.

Task is the base class for things to be done, added to a queue in the
Interpreter.

Interpreter contains the execution loop, dequeuing a Task and executing it
until the queue gets emptied.
"""

import contextlib
import itertools
from logging import getLogger
import traceback

from workflow_exec.module import FinishReason
from workflow_exec.looping import ModuleLoop


logger = getLogger('interpreter')


class InstantiatedModule(object):
    """A module in the workflow, that consumes inputs and produces outputs.

    This wraps the Module object defined by users.
    """
    _id_gen = itertools.count()

    def __init__(self, interpreter, class_):
        self.instance_id = next(self._id_gen)

        logger.debug("%r created, class=%r", self, class_)

        self._interpreter = interpreter
        self.up = {}
        self.down = {}
        self._class = class_
        self._instance = None

        # The input ports that are not done yet -- used to call all_input_end()
        self._inputs_producing = set()
        # The input this module has already requested, that we should deliver
        # to it as soon as it is available. Tasks have already been created for
        # these
        self._expected_input = {}
        # A call to module_produces_output() returned False, and an OutputTask
        # task has already been added to the interpreter
        self._expect_to_output = False
        # This module is done and shouldn't do anything anymore
        self._finished = False
        # Module's finish() was called
        self._finish_called = False

    def get_output_port(self, port):
        if port not in self.down:
            stream = self.down[port] = Stream(self._interpreter, self, port)
        else:
            stream = self.down[port]
        return stream

    def set_input_port(self, port, stream):
        assert port not in self.up
        self.up[port] = stream.new_consumer(self, port)
        if stream.producing:
            self._inputs_producing.add(port)

    @contextlib.contextmanager
    def _call_guard(self, description):
        try:
            yield
            if not (self._finished or
                    self._expected_input or self._expect_to_output):
                raise RuntimeError("%r isn't waiting for any event but isn't "
                                   "finished (%s)" % (self, description))
        except Exception:
            tb = traceback.format_exc()
            if not self._finish_called:
                logger.error("Terminating %r because it raised an exception "
                             "(%s):\n%s",
                             self, description, tb)
                try:
                    self.do_finish(FinishReason.TERMINATE)
                except Exception:
                    logger.error("Additionally, while terminating the module, "
                                 "the following exception was raised:\n%s",
                                 traceback.format_exc())
            else:
                logger.error("Got an exception from %r (%s):\n%s",
                             self, description, tb)

    def start(self):
        if self._instance is None:
            logger.debug("Adding StartTask for %r", self)
            self._interpreter.task_queue.append(StartTask(self))

    def do_start(self):
        if self._instance is not None:
            return

        logger.debug("%r starting", self)

        self._interpreter.started_modules.add(self)
        with self._call_guard("starting module"):
            self._instance = self._class(self)
            self._instance.start()

    def do_input(self, port, values):
        with self._call_guard("feeding input"):
            self._instance.input_list(port, values)

    def do_step(self):
        self._expect_to_output = False
        with self._call_guard("calling step"):
            self._instance.step()

    def do_finish(self, reason, raise_=False):
        if self._finish_called:
            return

        logger.debug("%r finished, reason=%s", self, reason)

        if reason != FinishReason.ALL_OUTPUT_DONE:
            # Close down streams
            for port, stream in self.down.iteritems():
                stream.close()

            # Close up streams
            for port, endpoint in self.up.iteritems():
                endpoint.close()

            # Remove module
            logger.debug("%r removed", self)
            self._interpreter.started_modules.remove(self)
            self._finished = self._finish_called = True
        if raise_:
            self._instance.finish(reason)
        else:
            with self._call_guard("calling finish"):
                self._instance.finish(reason)

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
        if self._finished:
            return
        self._finished = True

        logger.debug("%r reports finish", self)

        # Close down streams
        for port, stream in self.down.iteritems():
            stream.close()

        # Close up streams
        for port, endpoint in self.up.iteritems():
            endpoint.close()

        self._interpreter.task_queue.append(
            FinishTask(self, FinishReason.CALLED_FINISH))

    def upstream_end(self, port):
        logger.debug("stream finished: %r, input port %r", self, port)
        self._inputs_producing.remove(port)
        self._expected_input.pop(port, None)
        with self._call_guard("calling input_end"):
            self._instance.input_end(port)

            # If all upstream streams are done, do all_input_end()
            logger.debug("all input done")
            if not self._finished and not self._inputs_producing:
                self._instance.all_input_end()

    def downstream_end(self, port):
        logger.debug("stream finished: %r, output port %r", self, port)
        if self._finished:
            return
        if all(not stream.consumers for port, stream in self.down.iteritems()):
            logger.debug("every down stream is finished, delivering "
                         "ALL_OUTPUT_DONE")
            self._interpreter.task_queue.append(
                FinishTask(self, FinishReason.ALL_OUTPUT_DONE))

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

        # The producing module is waiting for this stream to clear out. An
        # OutputTask is to be created once space becomes available
        self.waiting = False

        # Whether this stream is still open; True until the producer finishes
        self.producing = True

        # Maps (module, port) to reading position
        self.consumers = set()
        self.position = 0

        # The buffer contains elements from `position` onwards
        self.buffer = []
        self.target_size = 1

    @classmethod
    def constants(cls, interpreter, values):
        stream = Stream(interpreter, None, None)
        stream.producing = False
        stream.buffer = values
        return stream

    @classmethod
    def constant(cls, interpreter, value):
        return cls.constants(interpreter, [value])

    def new_consumer(self, consumer_module, consumer_port):
        endpoint = StreamOutput(self, consumer_module, consumer_port)
        logger.debug("%r: new consumer: %r", self, endpoint)
        self.consumers.add(endpoint)
        return endpoint

    def push(self, values):
        if not self.consumers:
            return
        if not self.producing:
            raise RuntimeError("Attempt to write to a closed stream")

        self.buffer.extend(values)

        for endpoint in self.consumers:
            if endpoint.waiting:
                endpoint.waiting = False
                self.interpreter.task_queue.append(InputTask(endpoint))

        return len(self.buffer) < self.target_size

    def close(self):
        if not self.producing:
            return
        logger.debug("Closing %r", self)
        self.producing = False
        for endpoint in list(self.consumers):
            if endpoint.waiting:
                endpoint.waiting = False
                self.interpreter.task_queue.append(InputTask(endpoint))

    def remove_consumer(self, endpoint):
        if endpoint not in self.consumers:
            return
        self.consumers.remove(endpoint)
        if not self.consumers and self.producer_module is not None:
            self.producer_module.downstream_end(self.producer_port)

    def wait_output(self):
        if not self.waiting:
            logger.debug("%r.waiting = True", self)
        self.waiting = True

    def compact(self):
        # FIXME: optimize Stream#compact()
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

        if self.stream.producer_module is not None:
            self.stream.producer_module.start()

        if available - self.position > 0 or not self.stream.producing:
            self.stream.interpreter.task_queue.append(InputTask(self))
        if not self.requested:
            logger.debug("%r.waiting = True", self)
            self.waiting = True
        self.requested = True

    def close(self):
        logger.debug("Closing %r", self)
        self.stream.remove_consumer(self)

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
        self._module.do_start()

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
        all_available = module._expected_input.pop(port, None)
        if all_available is None:
            return
        logger.debug("endpoint: %d, stream: %d-%d",
                     endpoint.position,
                     stream.position, stream.position + len(stream.buffer))
        if (endpoint.position == stream.position + len(stream.buffer) and
                not stream.producing):
            module.upstream_end(port)
        elif all_available:
            logger.debug("Feeding all input to %r, port %r: "
                         "%d elements, %d-%d",
                         module, port,
                         stream.position + len(buffer) - endpoint.position,
                         endpoint.position,
                         stream.position + len(stream.buffer))
            feed = stream.buffer[endpoint.position - stream.position:]
            endpoint.position = stream.position + len(stream.buffer)
            module.do_input(port, feed)
        else:
            logger.debug("Feeding one input to %r, port %r: %d",
                         module, port, endpoint.position)
            feed = [stream.buffer[endpoint.position - stream.position]]
            endpoint.position += 1
            module.do_input(port, feed)

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
        self._module.do_step()

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
        self._module.do_finish(self._reason)

    def __repr__(self):
        return "FinishTask(module=%r, %r)" % (self._module, self._reason)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        # ####################
        # + FAKE PIPELINE
        # TODO: every port here is treated as depth=1 ports
        def connect(umod, uport, dmod, dport):
            stream = umod.get_output_port(uport)
            dmod.set_input_port(dport, stream)

        def constants(values, dmod, dport):
            stream = Stream.constants(self, values)
            dmod.set_input_port(dport, stream)

        m0 = InstantiatedModule(self, basic.ReadFile)
        constants(['/etc/resolv.conf'], m0, 'path')
        m1 = InstantiatedModule(self, basic.Count)
        connect(m0, 'line', m1, 'data')
        m2 = InstantiatedModule(self, basic.RandomNumbers)
        m3 = InstantiatedModule(self, basic.Zip)
        connect(m0, 'line', m3, 'left')
        connect(m2, 'number', m3, 'right')

        m4 = InstantiatedModule(self, basic.StandardOutput)
        connect(m1, 'length', m4, 'data')
        m5 = InstantiatedModule(self, basic.StandardOutput)
        connect(m3, 'zip', m5, 'data')

        sinks = [m4, m5]
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

            assert not self.started_modules
        finally:
            logger.debug("Execution done, killing remaining modules: %s",
                         ' '.join('%d' % m.instance_id
                                  for m in self.started_modules))
            for module in list(self.started_modules):
                module.do_finish(FinishReason.TERMINATE)
