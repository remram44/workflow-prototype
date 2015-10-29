class InstanciatedModule(object):
    def __init__(self, class_, parameters=None):
        if parameters is None:
            parameters = {}
        self.up = {}
        self.down = {}
        self._instance = class_(parameters, self)


class Interpreter(object):
    def execute_pipeline(self):
        import basic_modules as basic

        def connect(umod, uport, dmod, dport):
            umod.down[uport] = dmod, dport
            dmod.up[dport] = umod, uport

        a = InstanciatedModule(basic.Constant, {'value': '/etc/passwd'})
        b = InstanciatedModule(basic.ReadFile)
        connect(a, 'value', b, 'path')
        c = InstanciatedModule(basic.RandomNumbers)
        d = InstanciatedModule(basic.ParitySplitter)
        connect(c, 'number', d, 'number')
        e = InstanciatedModule(basic.Zip)
        connect(b, 'line', e, 'left')
        connect(d, 'odd', e, 'right')
        f = InstanciatedModule(basic.Sample, {'rate': 5})
        connect(e, 'zip', f, 'data')
        g = InstanciatedModule(basic.Sample, {'rate': 20})
        connect(d, 'even', g, 'data')
        h = InstanciatedModule(basic.AddPrevious)
        connect(g, 'sampled', h, 'number')
        i = InstanciatedModule(basic.Count)
        connect(h, 'sum', i, 'data')
        j = InstanciatedModule(basic.Format, {'format': "right branch ({0})"})
        connect(i, 'length', j, 'element')
        k = InstanciatedModule(basic.Format, {'format': "sum: {0}"})
        connect(h, 'sum', k, 'element')
        l = InstanciatedModule(basic.StandardOutput)
        connect(k, 'string', l, 'data')
        m = InstanciatedModule(basic.StandardOutput)
        connect(f, 'sampled', m, 'data')
        connect(j, 'string', m, 'data')
