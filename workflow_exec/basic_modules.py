from workflow_exec.module import Module


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

    def finish(self, reason):
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


class Sample(Module):
    """Lets one element through every 10 elements.
    """
    def start(self):
        self._pos = 1
        self._request_input('data')

    def input(self, port, value):
        if port == 'data':
            if self._pos == 10:
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
