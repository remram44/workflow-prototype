"""Current API version, v3.
"""

from workflow_exec.module import FinishReason, Module
from workflow_exec.inline_module import inline_module, \
    EndOfInput, FinishExecution, ExecutionTerminated, AllOutputDone


__all__ = ['FinishReason', 'Module',
           'inline_module', 'EndOfInput', 'FinishExecution',
           'ExecutionTerminated', 'AllOutputDone']
