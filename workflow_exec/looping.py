"""Module looping infrastructure.

A module's input or output port has a depth associated with it, in addition to
its type. For example, for type=Integer, depth=0 means that the port receives
or produces a single int value, depth=1 means a list of ints, depth=2 means a
list of lists of ints, ...

VisTrails allows ports of different depths to be connected together. When that
happens:
  - If the upstream port has a smaller depth than the downstream port, the
    values passed are wrapped into enough levels of lists to make it work, for
    example turning "1" into "[1]" if connecting depth=0 into depth=1
  - If the upstream port has a greater depth than the downstream port, the
    downstream modules will be set to automatically loop to handle the sequence
    of inputs. Therefore the apparent module sees all its ports' depth
    augmented, and execution will use the ModuleLoop wrapper of this package.
"""

from workflow_exec.module import Module


class ModuleLoop(Module):
    # TODO: ModuleLoop
    pass
