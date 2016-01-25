"""Support for v1 (with DeprecationWarning) and v2.
"""

import warnings

from workflow_exec.module import Module as NewModule
from .registrar import CompatibilityLayer


def deprecated(new_func):
    def wrapper(func):
        # TODO
        return func
    return wrapper


class ModuleError(Exception):
    # TODO
    pass


class ModuleSuspended(ModuleError):
    # TODO
    pass


class Module(object):
    """Base class for modules in VisTrails 2.x.

    This is one of the compatibility layers. It is implemented in terms of the
    current Module interface.
    """
    _adapter = None

    input_specs = {}
    output_specs = {}
    input_specs_order = []
    output_specs_order = []
    moduleInfo = {
        'locator': None,
        'controller': None,
        'vistrailName': 'Unknown',
        'version': -1,
        'pipeline': None,
        'moduleId': -1,
        'reason': 'Pipeline Execution',
        'actions': [],
    }

    is_cacheable = None
    update_upstream_port = None
    update_upstream = None
    update = None

    def compute(self):
        pass

    def get_input(self, port_name, allow_default=True):
        """Returns the value coming in on the input port named **port_name**.

        :param port_name: the name of the input port being queried
        :type port_name: str
        :param allow_default: whether to return the default value if it exists
        :type allow_default: bool
        :returns: the value being passed in on the input port
        :raises: ``ModuleError`` if there is no value on the port (and no default value if allow_default is True)

        """
        if port_name not in self.inputPorts:
            if allow_default and self.registry:
                defaultValue = self.get_default_value(port_name)
                if defaultValue is not None:
                    return defaultValue
            raise ModuleError(self, "Missing value from port %s" % port_name)

        # Cannot resolve circular reference here, need to be fixed later
        from vistrails.core.modules.sub_module import InputPort
        for conn in self.inputPorts[port_name]:
            if isinstance(conn.obj, InputPort):
                return conn()

        # Check for generator
        from vistrails.core.modules.basic_modules import Generator
        raw = self.inputPorts[port_name][0].get_raw()
        if isinstance(raw, Generator):
            return raw

        if self.input_specs:
            # join connections for depth > 0 and List
            list_desc = self.registry.get_descriptor_by_name(
                    'org.vistrails.vistrails.basic', 'List')

            if (self.input_specs[port_name].depth + self.list_depth > 0) or \
                self.input_specs[port_name].descriptors() == [list_desc]:
                ret = self.get_input_list(port_name)
                if len(ret) > 1:
                    ret = list(chain.from_iterable(ret))
                else:
                    ret = ret[0]
                return ret

        # else return first connector item
        value = self.inputPorts[port_name][0]()
        return value

    def get_input_list(self, port_name):
        """Returns the value(s) coming in on the input port named
        **port_name**.  When a port can accept more than one input,
        this method obtains all the values being passed in.

        :param port_name: the name of the input port being queried
        :type port_name: str
        :returns: a list of all the values being passed in on the input port
        :raises: ``ModuleError`` if there is no value on the port
        """

        if port_name not in self.inputPorts:
            raise ModuleError(self, "Missing value from port %s" % port_name)
        # Cannot resolve circular reference here, need to be fixed later
        from vistrails.core.modules.sub_module import InputPort
        fromInputPortModule = [connector()
                               for connector in self.inputPorts[port_name]
                               if isinstance(connector.obj, InputPort)]
        if len(fromInputPortModule)>0:
            return fromInputPortModule
        ports = []
        for connector in self.inputPorts[port_name]:
            from vistrails.core.modules.basic_modules import List, Variant
            value = connector()
            src_depth = connector.depth()
            if not self.input_specs:
                # cannot do depth wrapping
                ports.append(value)
                continue
            # Give List an additional depth
            dest_descs = self.input_specs[port_name].descriptors()
            dest_depth = self.input_specs[port_name].depth + self.list_depth
            if len(dest_descs) == 1 and dest_descs[0].module == List:
                dest_depth += 1
            if connector.spec:
                src_descs = connector.spec.descriptors()
                if len(src_descs) == 1 and src_descs[0].module == List and \
                   len(dest_descs) == 1 and dest_descs[0].module == Variant:
                    # special case - Treat Variant as list
                    src_depth -= 1
                if len(src_descs) == 1 and src_descs[0].module == Variant and \
                   len(dest_descs) == 1 and dest_descs[0].module == List:
                    # special case - Treat Variant as list
                    dest_depth -= 1
            # wrap depths that are too shallow
            while (src_depth - dest_depth) < 0:
                value = [value]
                src_depth += 1
            # type check list of lists
            root = value
            for i in xrange(1, src_depth):
                try:
                    # flatten
                    root = [item for sublist in root for item in sublist]
                except TypeError:
                    raise ModuleError(self, "List on port %s has wrong"
                                            " depth %s, expected %s." %
                                            (port_name, i-1, src_depth))

            if src_depth and root is not None:
                self.typeChecking(self, [port_name],
                                  [[r] for r in root] if src_depth else [[root]])
            ports.append(value)
        return ports

    def set_output(self, port_name, value):
        """This method is used to set a value on an output port.

        :param port_name: the name of the output port to be set
        :type port_name: str
        :param value: the value to be assigned to the port

        """
        self.outputPorts[port_name] = value

    def check_input(self, port_name):
        """check_input(port_name) -> None.
        Raises an exception if the input port named *port_name* is not set.

        :param port_name: the name of the input port being checked
        :type port_name: str
        :raises: ``ModuleError`` if there is no value on the port
        """
        if not self.has_input(port_name):
            raise ModuleError(self, "'%s' is a mandatory port" % port_name)

    def has_input(self, port_name):
        """Returns a boolean indicating whether there is a value coming in on
        the input port named **port_name**.

        :param port_name: the name of the input port being queried
        :type port_name: str
        :rtype: bool

        """
        return port_name in self.inputPorts

    def force_get_input(self, port_name, default_value=None):
        """Like :py:meth:`.get_input` except that if no value exists, it
        returns a user-specified default_value or None.

        :param port_name: the name of the input port being queried
        :type port_name: str
        :param default_value: the default value to be used if there is \
        no value on the input port
        :returns: the value being passed in on the input port or the default

        """

        if self.has_input(port_name):
            return self.get_input(port_name)
        else:
            return default_value

    def force_get_input_list(self, port_name):
        """Like :py:meth:`.get_input_list` except that if no values
        exist, it returns an empty list

        :param port_name: the name of the input port being queried
        :type port_name: str
        :returns: a list of all the values being passed in on the input port

        """
        if port_name not in self.inputPorts:
            return []
        return self.get_input_list(port_name)

    def get_default_value(self, port_name):
        reg = self.registry

        d = None
        try:
            d = reg.get_descriptor(self.__class__)
        except Exception:
            pass
        if not d:
            return None

        ps = None
        try:
            ps = reg.get_port_spec_from_descriptor(d, port_name, 'input')
        except Exception:
            pass
        if not ps:
            return None

        if len(ps.port_spec_items) == 1:
            psi = ps.port_spec_items[0]
            if psi.default is not None:
                m_klass = psi.descriptor.module
                return m_klass.translate_to_python(psi.default)
        else:
            default_val = []
            default_valid = True
            for psi in ps.port_spec_items:
                if psi.default is None:
                    default_valid = False
                    break
                m_klass = psi.descriptor.module
                default_val.append(
                    m_klass.translate_to_python(psi.default))
            if default_valid:
                return tuple(default_val)

        return None

    def annotate(self, d):

        """Manually add provenance information to the module's execution
        trace.  For example, a module that generates random numbers
        might add the seed that was used to initialize the generator.

        :param d: a dictionary where both the keys and values are strings
        :type d: dict

        """

        self.logging.annotate(self, d)

    def job_monitor(self):
        """ job_monitor() -> JobMonitor
        Returns the JobMonitor for the associated controller if it exists
        """
        if 'job_monitor' not in self.moduleInfo or \
           not self.moduleInfo['job_monitor']:
            raise ModuleError(self,
                              "Cannot run job, no job_monitor is specified!")
        return self.moduleInfo['job_monitor']

    @deprecated("get_input")
    def getInputFromPort(self, *args, **kwargs):
        if 'allowDefault' in kwargs:
            kwargs['allow_default'] = kwargs.pop('allowDefault')
        return self.get_input(*args, **kwargs)

    @deprecated("get_input_list")
    def getInputListFromPort(self, *args, **kwargs):
        return self.get_input_list(*args, **kwargs)

    @deprecated("force_get_input")
    def forceGetInputFromPort(self, *args, **kwargs):
        return self.force_get_input(*args, **kwargs)

    @deprecated("force_get_input_list")
    def forceGetInputListFromPort(self, *args, **kwargs):
        return self.force_get_input_list(*args, **kwargs)

    @deprecated("has_input")
    def hasInputFromPort(self, *args, **kwargs):
        return self.has_input(*args, **kwargs)

    @deprecated("check_input")
    def checkInputPort(self, *args, **kwargs):
        return self.check_input(*args, **kwargs)

    @deprecated("set_output")
    def setResult(self, *args, **kwargs):
        return self.set_output(*args, **kwargs)

    @deprecated("get_input_connector")
    def getInputConnector(self, *args, **kwargs):
        return self.get_input_connector(*args, **kwargs)

    @deprecated("get_default_value")
    def getDefaultValue(self, *args, **kwargs):
        return self.get_default_value(*args, **kwargs)

    @deprecated("enable_output_port")
    def enableOutputPort(self, *args, **kwargs):
        return self.enable_output_port(*args, **kwargs)

    @deprecated("remove_input_connector")
    def removeInputConnector(self, *args, **kwargs):
        return self.remove_input_connector(*args, **kwargs)


class NotCacheable(object):
    def is_cacheable(self):
        return False


class Streaming(object):
    # TODO
    pass


class Converter(object):
    # TODO
    pass


def new_module():
    # TODO
    pass


class Version2Adapter(NewModule):
    _orig_class = None
    _orig_instance = None

    def __init__(self, *args, **kwargs):
        NewModule.__init__(self, *args, **kwargs)
        self._orig_instance = self._orig_class()
        self._orig_instance._adapter = self


_wrapped = {}


def version2_adapter(module_class):
    if not issubclass(module_class, Module):
        return None
    if module_class in _wrapped:
        return _wrapped[module_class]

    if hasattr(module_class, 'transfer_attrs'):
        warnings.warn("API v2 Module uses transfer_attrs: %r. This is "
                      "probably not going to work as expected", module_class)
    for bad_field in ('update_upstream', 'updateUpstream', 'update_upstream_port', 'updateUpstreamPort', 'update'):
        if getattr(module_class, bad_field, None) is not None:
            warnings.warn("API v2 Module uses %s: %r. This "
                          "is not going to work as expected",
                          bad_field, module_class)
    # TODO: is_cacheable

    adapter = type(module_class.__name__, (Version2Adapter,),
                   {'_orig_class': module_class})
    return adapter


CompatibilityLayer.register_adapter(version2_adapter)


__all__ = ['ModuleError', 'ModuleSuspended', 'Module', 'NotCacheable',
           'Streaming', 'Converter', 'new_module']
