"""Compatibility alias for package variants that import ``_session_guard``."""

from .private_chat_session_guard import (
    emit_plugin_metadata,
    guard_session,
    sample_guard,
)

__all__ = ["emit_plugin_metadata", "guard_session", "sample_guard"]
