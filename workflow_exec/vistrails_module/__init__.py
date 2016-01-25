"""API for Module development.

This package contains the different versions of the Module API, on top of which
Modules are implemented. It is versioned so that changes in the API or the
interpreter are possible without breaking existing package code (just create a
new version).
"""

# Version history:
#   - v1: used until VisTrails 2.1.x
#       - was sometimes used as data ('self' port)
#   - v2: released in VisTrails 2.2.0
#       - renamed methods to snake_case (getInputFromPort -> get_input, ...)
#       - had a sort of streaming, NOW UNSUPPORTED
#       note: supported by v2.Module with DeprecationWarning

# Modules that import from here directly get v2
from .v2 import Module
