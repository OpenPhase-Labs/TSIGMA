"""A plugin borrows the caller's identity for one invocation, and no longer.

Contract ADR-0006 (amended): a plugin gets no DB credentials and no schema, and
reads through the host's own policy-governed API. The credential it reads with
is minted as the user the report is being run for and revoked when the call
ends, so a plugin reaches exactly what that user reaches - never more, and never
after the request that lent it.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tsigma.plugins.credentials import (
    credential_metadata,
    current_caller,
    plugin_credential,
    set_invocation_caller,
)

from tsigma.auth.sessions import SessionData


def _user(username="opsjim", role="viewer"):
    now = datetime.now(timezone.utc)
    return SessionData(
        user_id=uuid4(), username=username, role=role,
        created_at=now, expires_at=now + timedelta(hours=1),
    )


def _store(token="tok-1"):
    store = AsyncMock()
    store.create = AsyncMock(return_value=token)
    store.delete = AsyncMock()
    return store


@pytest.fixture(autouse=True)
def _clear_context():
    set_invocation_caller(None, None)
    yield
    set_invocation_caller(None, None)


class TestTheCredentialIsTheCallers:
    @pytest.mark.asyncio
    async def test_it_is_minted_as_the_requesting_user(self):
        user, store = _user(), _store()
        set_invocation_caller(user, store)

        async with plugin_credential() as token:
            assert token == "tok-1"

        store.create.assert_awaited_once_with(user.user_id, user.username, user.role)

    @pytest.mark.asyncio
    async def test_two_callers_do_not_share_a_credential(self):
        first, second, store = _user("alice"), _user("bob"), _store()

        set_invocation_caller(first, store)
        async with plugin_credential():
            pass
        set_invocation_caller(second, store)
        async with plugin_credential():
            pass

        minted_for = [call.args[1] for call in store.create.await_args_list]
        assert minted_for == ["alice", "bob"], (
            "a plugin must not inherit whoever happened to call first"
        )


class TestItDoesNotOutliveTheInvocation:
    @pytest.mark.asyncio
    async def test_it_is_revoked_on_the_way_out(self):
        store = _store()
        set_invocation_caller(_user(), store)

        async with plugin_credential() as token:
            pass

        store.delete.assert_awaited_once_with(token)

    @pytest.mark.asyncio
    async def test_it_is_revoked_even_when_the_plugin_fails(self):
        store = _store()
        set_invocation_caller(_user(), store)

        with pytest.raises(RuntimeError):
            async with plugin_credential():
                raise RuntimeError("plugin died mid-stream")

        store.delete.assert_awaited_once(), (
            "a credential surviving a crash is a standing grant nobody issued"
        )

    @pytest.mark.asyncio
    async def test_a_revoke_failure_does_not_break_the_report(self):
        store = _store()
        store.delete = AsyncMock(side_effect=RuntimeError("valkey is down"))
        set_invocation_caller(_user(), store)

        async with plugin_credential() as token:
            assert token == "tok-1"
        # No exception: the TTL is the backstop.


class TestAnAnonymousRequestBorrowsNothing:
    @pytest.mark.asyncio
    async def test_no_caller_means_no_credential(self):
        # A public category the operator opened. The plugin reads the API as an
        # anonymous client would - the same answer the operator already gave.
        set_invocation_caller(None, _store())
        async with plugin_credential() as token:
            assert token is None

    @pytest.mark.asyncio
    async def test_no_store_means_no_credential(self):
        set_invocation_caller(_user(), None)
        async with plugin_credential() as token:
            assert token is None


class TestTheCredentialTravelsAsMetadata:
    def test_a_token_becomes_a_bearer_header(self):
        assert credential_metadata("tok-1") == [("authorization", "Bearer tok-1")]

    def test_no_token_sends_no_metadata(self):
        assert credential_metadata(None) == []

    def test_the_caller_is_readable_for_diagnostics(self):
        user, store = _user(), _store()
        set_invocation_caller(user, store)
        assert current_caller() is user
