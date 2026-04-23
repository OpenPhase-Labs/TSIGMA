"""
Ingestion method plugins auto-discovery.

Automatically imports all method modules to trigger @IngestionMethodRegistry.register decorators.
"""

from pathlib import Path

# Auto-discover and import all ingestion method modules
methods_dir = Path(__file__).parent
for module_file in methods_dir.glob("*.py"):
    if module_file.stem != "__init__":
        __import__(f"tsigma.collection.methods.{module_file.stem}")
