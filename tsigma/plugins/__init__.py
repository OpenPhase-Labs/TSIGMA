"""gRPC plugin host runtime.

Importing this package puts the generated stub tree on ``sys.path`` so the
generated modules resolve their own cross-imports (``tsigma.method.v1`` imports
``google.protobuf``; the go-plugin modules import each other by bare name).
Mirrors the pattern in ``tsigma/collection/methods/grpc_server.py``.

``gen/tsigma/`` is deliberately a namespace package - an ``__init__.py`` there
would shadow this very package.
"""

import sys
from pathlib import Path

_GEN_DIR = str(Path(__file__).parent / "gen")
if _GEN_DIR not in sys.path:
    sys.path.insert(0, _GEN_DIR)
