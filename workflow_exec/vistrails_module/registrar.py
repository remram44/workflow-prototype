from workflow_exec.module import Module


class CompatibilityLayer(object):
    def __init__(self):
        self._adapters = set()

    def register_adapter(self, adapter):
        self._adapters.add(adapter)

    def wrap(self, module_class):
        if issubclass(module_class, Module):
            return module_class
        for adapter in self._adapters:
            res = adapter(module_class)
            if res is not None:
                return res
        raise TypeError("Don't know how to wrap %r into a Module" %
                        module_class)


CompatibilityLayer = CompatibilityLayer()
