"""Production boundary for the project's standardization functions.

The supplied project references `function_mapping` but does not supply the
implementation of the referenced standardization functions. Therefore this
module intentionally does not invent business rules.

When the approved implementation is deployed, it can be exposed through the
supported Databricks module import below. Until then, configured rule names
fail explicitly in the standardization layer rather than being silently
skipped or replaced with guessed transformations.
"""

from __future__ import annotations

try:
    from databricks.src.notebooks.standardization_function import function_mapping
except (ImportError, ModuleNotFoundError):
    function_mapping = {}

if not isinstance(function_mapping, dict):
    raise TypeError("standardization_function.function_mapping must be a dict")

__all__ = ["function_mapping"]
