"""Pinned TSIGMA Plugin Protocol values.

Normative source is TSIGMA-Contract PROTOCOL.md; these are the Python mirror.
Changing a value here without changing the contract breaks the handshake.
"""

# go-plugin envelope version. PROTOCOL.md: CORE-PROTOCOL-VERSION = 1, pinned.
CORE_PROTOCOL_VERSION = 1

# TSIGMA Plugin Protocol version, carried as APP-PROTOCOL-VERSION.
PLUGIN_PROTOCOL_VERSION = 1

# Handshake guard, not a secret. A plugin started without this env var set to
# MAGIC_COOKIE_VALUE exits immediately.
MAGIC_COOKIE_KEY = "TSIGMA_PLUGIN_MAGIC"
MAGIC_COOKIE_VALUE = "tsigma-plugin-v1"

# Subsystem contracts generated into gen/. auth and storage are excluded: their
# proto packages collide with this repo's real tsigma/auth/ and tsigma/storage/
# packages, and both subsystems are deferred pending the privileged tier.
GENERATED_SUBSYSTEMS = ("decoder", "method", "notify", "report")
