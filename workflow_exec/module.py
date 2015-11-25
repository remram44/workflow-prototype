class FinishReason(object):
    TERMINATE = 1
    ALL_OUTPUT_DONE = 2
    CALLED_FINISH = 3


class Module(object):
    """Base class for Python module implementations.
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
        `_finish()`. If this module is a collecter, it has received all the
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
