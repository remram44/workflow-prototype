from workflow_exec.module import FinishReason, Module


class Constant(Module):
    """Outputs a single value set as parameter.
    """
    def start(self):
        self._output('value', self.parameters['value'])
        self._finish()


class Count(Module):
    """Counts the number of elements in the input stream.
    """
    def start(self):
        self.counter = 0
        self._request_input('data')

    def input(self, port, value):
        if port == 'data':
            self.counter += 1
            self._request_input('data')
        else:
            super(Count, self).input(port, value)

    def all_input_end(self):
        self._output('length', self.counter)


class Zip(Module):
    """Builds pairs from two streams.

    Ends when either one of the streams ends.
    """
    def start(self):
        self.left = self.right = False, None
        self._request_input('left')
        self._request_input('right')

    def input(self, port, value):
        if port == 'left':
            assert not self.left[0]
            self.left = True, value
        elif port == 'right':
            assert not self.right[0]
            self.right = True, value
        else:
            super(Zip, self).input(port, value)

        if self.left[0] and self.right[0]:
            self._output('zip', (self.left[1], self.right[1]))
            self.left = self.right = False, None
            self._request_input('left')
            self._request_input('right')

    def input_end(self, port):
        self._finish()


class ReadFile(Module):
    """Opens a file and output its lines.
    """
    def start(self):
        self._file = None
        self._request_input('path')

    def input(self, port, value):
        if port == 'path':
            assert self._file is None
            self._file = open(value)
            self.step()
        else:
            super(ReadFile, self).input(port, value)

    def step(self):
        while True:
            line = self._file.readline()
            if not line:
                self._finish()
                break
            # _output returns False if no more output should be produced for
            # the moment
            if not self._output('line', line):
                break

    def finish(self, reason):
        self._file.close()


class RandomNumbers(Module):
    """Outputs random numbers forever.
    """
    def start(self):
        import random
        self._generator = random.Random()
        self.step()

    def step(self):
        # _output returns False if no more output should be produced for the
        # moment
        while self._output('number', self._generator.randint(0, 255)):
            pass

    def finish(self, reason):
        if reason == FinishReason.ALL_OUTPUT_DONE:
            self._finish()


class Sample(Module):
    """Lets one element through every N elements (configured with parameter).
    """
    def start(self):
        self._pos = 1
        self._rate = self.parameters['rate']
        self._request_input('data')

    def input(self, port, value):
        if port == 'data':
            if self._pos == self._rate:
                self._pos = 1
                self._output('sampled', value)
            else:
                self._pos += 1
            self._request_input('data')
        else:
            super(Sample, self).input(port, value)


class StandardOutput(Module):
    """Outputs input values to stdout.
    """
    def start(self):
        self._request_input('data')

    def input(self, port, value):
        if port == 'data':
            print(value)
            self._request_input('data')
        else:
            super(StandardOutput, self).input(port, value)

    def all_input_end(self):
        self._finish()


class ParitySplitter(Module):
    """Separates even and odd values.
    """
    def start(self):
        self._request_input('number')

    def input(self, port, value):
        if port == 'number':
            if value % 2 == 0:
                self._output('even', value)
            else:
                self._output('odd', value)
            self._request_input('number')
        else:
            super(ParitySplitter, self).input(port, value)


class AddPrevious(Module):
    """Maps an element to the sum of all elements up to it.
    """
    def start(self):
        self._sum = 0
        self._request_input('number')

    def input(self, port, value):
        if port == 'number':
            self._sum += value
            self._output('sum', self._sum)
            self._request_input('number')
        else:
            super(AddPrevious, self).input(port, value)


class Format(Module):
    """Formats elements using format string from parameters.
    """
    def start(self):
        self._format = self.parameters['format']
        self._args = []
        self._request_input('element')

    def input(self, port, value):
        if port == 'element':
            self._args.append(value)
            self._request_input('element')
        else:
            super(Format, self).input(port, value)

    def input_end(self, port):
        self._output('string', self._format.format(*self._args))


from workflow_exec.inline_module import inline_module, EndOfInput


@inline_module
def ZipLongest(module, parameters):
    try:
        while True:
            left, right = yield module.get_input('left', 'right')
            yield module.output('zip', (left, right))
    except EndOfInput, e:
        if 'left' in e.ports:
            while True:
                right = yield module.get_input('right')
                yield module.output('zip', (None, right))
        else:
            while True:
                left = yield module.get_input('left')
                yield module.output('zip', (left, None))
