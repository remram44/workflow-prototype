"""Module interface.

This is the interface that Modules implement to be executable by the VisTrails
interpreter. It is exposed as vistrails_module.v3, and previous versions are
implemented on top of this (as adaptors).
"""

class FinishReason(object):
    """Enumeration of reasons, passed to Module#finish().

    This indicates why the Module is done.

    TERMINATE means that it was forcibly terminated by the interpreter, either
    because of an error (from this module or another) or by user request.
    CALLED_FINISH means that this module called _finish() and thus completed
    normally; some cleaning up can be done here before execution ends.
    ALL_OUTPUT_DONE means that no downstream module is reading what this module
    is outputting anymore, and while it is unusual for modules to act this way,
    we can finish execution faster by stopping execution here.

    Note: if this modules chooses to terminate early after ALL_OUTPUT_DONE, it
    should still call _finish(). finish(CALLED_FINISH) will then be triggered.
    """
    TERMINATE = 1
    ALL_OUTPUT_DONE = 2
    CALLED_FINISH = 3


class Module(object):
    """Base class for Python module implementations.

    This is basically a stream processor, producing output as input is
    received.

    The following methods can be used:
      - _request_input() to ask for input on a specific port (no input will be
        delivered to the Module unless this has been called first)
      - _output() to produce an output on a specific port (returns False if we
        should stop outputting to let other modules consume, and resume
        production in step())
      - _output_list() to produce a list of outputs instead of a single element
      - _finish() to signal this module is done. Connected modules will get
        notified and pipeline execution will end once each module has finished.

    The following methods can be implemented:
      - start() is the entry point, which should probably call _request_input()
        for the input ports so that input() gets called back.
      - input() provides the module with data from an input port.
      - input_list() provides the module with a list of elements from an input
        port, and allows modules to handle a full chunk of data at once. Always
        faster than waiting for elements one by one, especially useful when
        vectorizing (e.g. using numpy arrays)
      - input_end() indicates that an input port is done (upstream module is
        finished)
      - all_input_end() indicates that all input ports are done (input_end()
        have been called for all of them)
      - step() requests more output, and will be called if _output() ever
        returned False.
      - finish() is called when this module ends, and can be used to free
        resources, close files, ...
    """
    def __init__(self, parameters, interface):
        """Constructor.

        Modules shouldn't do any work here, and you should forward arguments to
        the base class if overriding it (or just use `start()` instead).
        """
        self.parameters = parameters
        self._interface = interface

    def start(self):
        """Start execution.

        This starts executing the module. You can read parameters, request
        input and start output from here, then rely on the other events the
        interpreter will trigger later.
        """
        raise NotImplementedError

    def input(self, port, value):
        """Handle an input on a port.
        """
        raise KeyError("Unexpected input %r" % port)

    def input_list(self, port, values):
        """Handle a list of input on a port.

        The default implementation just loops and calls `input()`.
        """
        for v in values:
            self.input(port, v)

    def input_end(self, port):
        """Handle the end of the input on a port.
        """

    def all_input_end(self):
        """There is no port left they might still receive input.

        If this module produces data in `input()`, you should probably call
        `_finish()`. If this module is a collector, it has received all the
        input and can now produce its result.
        """

    def step(self):
        """Produce more output.

        This is called by the interpreter if `_output()` was called and
        returned False.
        """
        # If this is called and the input streams are done, stop execution
        self._interface.module_step_unimplemented()

    def finish(self, reason):
        """Ends execution of the module.

        `reason` can be either:
        * `TERMINATE`: there was an error or the execution was aborted, the
          module should finish quickly. Any more output will likely get
          ignored.
        * `ALL_OUTPUT_DONE`: the output streams have no more readers. This
          module can safely call `_finish()` early, else it will keep getting
          called.
        * `CALLED_FINISH`: you called `_finish()`.
        """

    def _request_input(self, port, all_available=False):
        """Ask for input on a port.

        This indicates that the module expects data on the given port next.
        `input()` (or `input_end()`) will be called when it is available.

        Don't forget to call this!

        If all_available is set, the module will get all the input currently
        available at once instead of buffering up until the next
        `_request_input()` call.
        """
        self._interface.module_requests_input(port, all_available)

    def _output(self, port, value):
        """Output data on a port.

        This sends a value to the downstream modules connected to that port.

        This methods returns False if no more data should be produced (for
        example because a buffer was filled, the downstream modules don't read
        fast enough). In that case, there is no need to resend that element;
        it is also safe to ignore the return value, the only cost is greater
        memory usage and latency.
        """
        return self._interface.module_produces_output(port, [value])

    def _output_list(self, port, values):
        """Output data on a port.

        This sends multiple elements to the downstream modules at once.

        This is more efficient than `_output()` in some conditions.
        """
        return self._interface.module_produces_output(port, values)

    def _finish(self):
        """Indicates that this module is done.

        The downstream module will be notified of the end of the streams. It is
        an error to do anything after `finish()` has been called.
        """
        self._interface.module_reports_finish()
