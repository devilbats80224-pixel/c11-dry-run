"""Process-local context for native Freqtrade resolver callbacks."""
from contextvars import ContextVar

current = ContextVar('c11_runtime_context', default=None)
