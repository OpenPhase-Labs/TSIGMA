"""
Auth provider plugins auto-discovery.

Automatically imports all provider modules to trigger
@AuthProviderRegistry.register decorators.
"""

from pathlib import Path

providers_dir = Path(__file__).parent
for _module_file in providers_dir.glob("*.py"):
    if _module_file.stem != "__init__":
        __import__(f"tsigma.auth.providers.{_module_file.stem}")
