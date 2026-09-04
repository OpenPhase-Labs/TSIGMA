"""Short-lived credentials for out-of-process plugins.

A report plugin needs data. It does not get a database session, a DSN or a
schema (contract ADR-0006) - it reads through the host's own HTTP API, which is
the surface that already applies the operator's `access_policy` per category.

The credential it reads with is minted **as the user the report is being run
for**, lives only for that one invocation, and is revoked when the invocation
ends. Two consequences worth stating, because they are the whole point:

- A plugin sees exactly what that user can see. It cannot be granted a
  standing identity of its own, and it cannot outlive the request. There is no
  second notion of "what a plugin may read" to keep in sync with the first.
- A host-served query service answering from the host's own session bypasses
  `access_policy` entirely - a vendor binary would reach data an unauthenticated
  human requesting the same thing would be refused. Minting as the caller
  inverts that: the plugin is bounded by the operator's policy, for one call.

The caller travels in a ContextVar rather than through `Report.execute`, because
that signature is shared with every in-process report and none of them needs it.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar

from ..auth.sessions import BaseSessionStore, SessionData

logger = logging.getLogger(__name__)

# Minutes a plugin credential stays valid. Short on purpose: it exists for the
# length of one Generate call, and the revoke below is the real bound - this is
# only the backstop for a host that dies mid-invocation and never revokes.
PLUGIN_CREDENTIAL_TTL_MINUTES = 5

_caller: ContextVar[SessionData | None] = ContextVar("plugin_caller", default=None)
_store: ContextVar[BaseSessionStore | None] = ContextVar("plugin_store", default=None)


def set_invocation_caller(user: SessionData | None, store: BaseSessionStore | None) -> None:
    """Record who this request is for, so a plugin can be lent their identity."""
    _caller.set(user)
    _store.set(store)


def current_caller() -> SessionData | None:
    return _caller.get()


@asynccontextmanager
async def plugin_credential():
    """Mint a credential for one plugin invocation, then revoke it.

    Yields None when there is no caller to borrow from - an unauthenticated
    request against a category the operator has made public. A plugin then reads
    the API exactly as an anonymous client would, which is the same answer the
    operator already gave for that data.
    """
    user = _caller.get()
    store = _store.get()
    if user is None or store is None:
        yield None
        return

    token = await store.create(user.user_id, user.username, user.role)
    try:
        yield token
    finally:
        # Revoked whether the plugin succeeded, failed or hung up. A credential
        # that outlives its invocation is a standing grant nobody issued.
        try:
            await store.delete(token)
        except Exception:
            logger.exception(
                "failed to revoke the plugin credential for %s; it expires in "
                "%d minutes regardless",
                user.username, PLUGIN_CREDENTIAL_TTL_MINUTES,
            )


def credential_metadata(token: str | None) -> list[tuple[str, str]]:
    """gRPC call metadata carrying the credential.

    Metadata, not a field on `GenerateRequest`: the contract's request message
    has no credential field, and putting a bearer token in a logged request body
    is how it ends up in a trace.
    """
    return [("authorization", f"Bearer {token}")] if token else []
