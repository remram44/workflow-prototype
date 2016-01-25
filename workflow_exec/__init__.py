"""Module scheduling and execution logic.

This is the interpreter package for VisTrails, which executes modules and
passes data around as it is requested and produced.

The 'module' module contains the definition of Module, the base interface that
VisTrails module implement and that the interpreter interacts with. It has
facilities for producing outputs and requesting inputs.

The 'interpreter' module contains the actual scheduling logic, instanciating a
pipeline and executing modules in order so as to stream data from module to
module until every module is done.

The 'basic_modules' module contains some basic modules that implement the
Module interface and is currently used for testing purposes.
TODO: once this is integrated in VisTrails, they should be replaced by the
basic_modules package, to which some of the present modules will be added.
"""

from workflow_exec.interpreter import Interpreter
