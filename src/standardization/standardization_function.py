"""
Standardization function boundary.

The supplied project references:
    standardization_function.function_mapping

The actual implementation of the configured standardization functions was
not supplied with the project files.

Therefore this module intentionally does NOT invent business rules.

If the approved external implementation is available in the Databricks
environment, it is loaded. Otherwise function_mapping remains empty and
standardization.py will fail clearly when an actual configured rule requires
a missing function.
"""

from __future__ import annotations

function_mapping = {}

# Try the legacy supplied-project location only when it is actually available.
try:
    from databricks.src.notebooks.standardization_function import (
        function_mapping as _external_function_mapping
    )

    if isinstance(_external_function_mapping, dict):
        function_mapping = _external_function_mapping

except (ImportError, ModuleNotFoundError):
    pass

__all__ = ["function_mapping"]