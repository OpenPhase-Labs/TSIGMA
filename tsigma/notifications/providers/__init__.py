"""
Notification provider plugins auto-discovery.

Automatically imports all provider modules to trigger
@NotificationRegistry.register decorators.
"""

from pathlib import Path

providers_dir = Path(__file__).parent
for _module_file in providers_dir.glob("*.py"):
    if _module_file.stem != "__init__":
        __import__(f"tsigma.notifications.providers.{_module_file.stem}")
