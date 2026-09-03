"""Compatibility boundary for the project's external standardization functions.

The supplied project references ``standardization_function.function_mapping`` but
does not provide that module's implementation. This module deliberately does not
invent rule functions. When the external implementation is available in the
Databricks deployment, it is loaded; otherwise the mapping remains empty and the
standardization pipeline reports the missing configured rule at execution time.
"""

from __future__ import annotations

try:
    from databricks.src.notebooks.standardization_function import function_mapping
except (ImportError, ModuleNotFoundError):
    function_mapping = {}

__all__ = ["function_mapping"]

# ============================================================================
# USER CONFIGURATION
# ============================================================================
# This module contains reusable standardization functions. No credentials belong
# here. Rule names/selection come from the project standardization configuration.
# ============================================================================

