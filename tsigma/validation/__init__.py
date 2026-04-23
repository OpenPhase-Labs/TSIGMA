"""
TSIGMA Validation Framework.

Post-ingestion event validation with a plugin SDK.
Layer 1 (schema/range) is built-in; Layers 2/3 are external plugins.
"""

import tsigma.validation.validators  # noqa: F401 — triggers auto-discovery
