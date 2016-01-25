"""API for Module development.

This package contains the different versions of the Module API, on top of which
Modules are implemented. It is versioned so that changes in the API or the
interpreter are possible without breaking existing package code (just create a
new version). A wrapper is used to wrap these into the current Module
interface, so that we don't have to do too much magic with metaclasses;
therefore specific versions register themselves with the CompatibilityLayer
singleton.
"""

# Version history:
#   - v1: used until VisTrails 2.1.x
#       - was sometimes used as data ('self' port)
#   - v2: released in VisTrails 2.2.0
#       - renamed methods to snake_case (getInputFromPort -> get_input, ...)
#       - had a sort of streaming, NOW UNSUPPORTED
#       note: supported by v2.Module with DeprecationWarning

# Modules that import from here directly get v2
from .v2 import ModuleError, ModuleSuspended, Module, NotCacheable, \
    Streaming, Converter, new_module

from .registrar import CompatibilityLayer
