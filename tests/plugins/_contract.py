"""Read published values out of the sibling TSIGMA-Contract repo.

The contract - not this repo - is normative for the handshake. Tests that need a
published value read it here rather than restating it, and skip when the sibling
repo is not checked out so the suite still runs without it.

Not a test module: pytest does not collect ``_contract.py``.
"""

import os
import re
from pathlib import Path

import pytest

# Same env var and default scripts/gen_proto.sh honors, so a relocated contract
# resolves identically. VERSION and PROTOCOL.md live one level above proto/.
DEFAULT_CONTRACT_PROTO = "/opt/webpages/TSIGMA-Contract/proto"
CONTRACT_PROTO = Path(os.environ.get("TSIGMA_CONTRACT_PROTO", DEFAULT_CONTRACT_PROTO))
CONTRACT_ROOT = CONTRACT_PROTO.parent

# - `NETWORK` = `tcp` or `unix`.
NETWORK_BULLET_RE = re.compile(r"`NETWORK`\s*=\s*([^.]*)")
# - `PROTOCOL` = `grpc` (the only supported value; ...)
PROTOCOL_BULLET_RE = re.compile(r"`PROTOCOL`\s*=\s*`([^`]+)`")
BACKTICKED_RE = re.compile(r"`([^`]+)`")


def require_contract() -> Path:
    """Return the contract repo root, skipping when it is not checked out."""
    if not CONTRACT_PROTO.is_dir():
        pytest.skip(f"contract protos not found at {CONTRACT_PROTO}; set TSIGMA_CONTRACT_PROTO")
    return CONTRACT_ROOT


def read_contract_file(relative: str) -> str:
    root = require_contract()
    path = root / relative
    assert path.is_file(), f"contract publishes no {relative} at {path}"
    return path.read_text(encoding="utf-8")


def protocol_section_one() -> str:
    """PROTOCOL.md section 1, whitespace-normalized so bullets can wrap lines."""
    text = read_contract_file("PROTOCOL.md")
    match = re.search(r"^## 1\.[^\n]*\n(.*?)(?=^## )", text, re.MULTILINE | re.DOTALL)
    assert match, "PROTOCOL.md has no '## 1.' section; the handshake values moved"
    return re.sub(r"\s+", " ", match.group(1))


def permitted_networks() -> set[str]:
    """The NETWORK values PROTOCOL.md section 1 permits."""
    section = protocol_section_one()
    bullet = NETWORK_BULLET_RE.search(section)
    assert bullet, "PROTOCOL.md section 1 no longer publishes the NETWORK bullet"
    values = set(BACKTICKED_RE.findall(bullet.group(1)))
    assert values, f"no backticked NETWORK values in {bullet.group(1)!r}"
    return values


def permitted_protocol() -> str:
    """The single PROTOCOL value PROTOCOL.md section 1 permits."""
    section = protocol_section_one()
    bullet = PROTOCOL_BULLET_RE.search(section)
    assert bullet, "PROTOCOL.md section 1 no longer publishes the PROTOCOL bullet"
    return bullet.group(1)
