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

    def input_end(self, port):
        """Handle the end of the input on a port.
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

        This doesn't necessary means that the module is finish.

        `reason` can be either:
        * `ALL_INPUT_DONE`: all the input streams are done. If this module
          produces data in `input()`, you should probably call `_finish()`. If
          this module is a collecter, it has received all the input and can now
          produce its result.
        * `TERMINATE`: there was an error or the execution was aborted, the
          module should finish quickly. Any more output will likely get
          ignored.
        * `ALL_OUTPUT_DONE`: the output streams have no more readers. This
          module can safely call `_finish()` early.

        Note that it is safe to continue producing output after this, or from
        this method. `step()` will continue getting called if `output()`
        returns False.
        """

    def _request_input(self, port, nb=1):
        """Ask for input on a port.

        This indicates that the module expects data on the given port next.
        `input()` (or `input_end()`) will be called when it is available.

        Don't forget to call this!
        """
        self._interface.module_requests_input(port, nb)

    def _output(self, port, value):
        """Output data on a port.

        This sends a value to the downstream modules connected to that port.

        This methods returns False if no more data should be produced (for
        example because a buffer was filled, the downstream modules don't read
        fast enough). In that case, there is no need to resend that element;
        it is also safe to ignore the return value, the only cost is greater
        memory usage and latency.
        """
        return self._interface.module_produces_output(port, value)

    def _finish(self):
        """Indicates that this module is done.

        The downstream module will be notified of the end of the streams. It is
        an error to do anything after `finish()` has been called.
        """
        self._interface.module_reports_finish()
