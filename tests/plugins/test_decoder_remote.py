"""Phase 6 gate: the decoder slice.

Bidi Decode against a real plugin subprocess, the 3-state terminal outcome, and
the host-side naive-local -> UTC conversion with DST fold resolution.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tsigma.collection.decoders.base import BaseDecoder, DecodedEvent
from tsigma.plugins.connection import LaunchedConnection
from tsigma.plugins.remote_decoder import (
    CAN_DECODE_HEAD_BYTES,
    DecodeOutcome,
    RemoteDecodeError,
    RemoteDecoder,
    arrow_batch_to_events,
    chunk_bytes,
    events_to_arrow_batch,
    resolve_naive_local,
)

PLUGIN = str(Path(__file__).parent / "fake_decoder_plugin.py")
NY = "America/New_York"


async def _decoder(*args: str):
    conn = LaunchedConnection("fake-asc3", [sys.executable, PLUGIN, *args])
    await conn.connect()
    from tsigma.decoder.v1 import decoder_pb2, decoder_pb2_grpc

    stub = decoder_pb2_grpc.DecoderStub(conn.channel)
    d = await stub.Describe(decoder_pb2.DescribeRequest())
    return (
        RemoteDecoder(conn, d.name, list(d.extensions), d.description,
                      d.time_semantics, d.output_kind),
        conn,
    )


class TestChunking:
    def test_splits_into_bounded_chunks(self):
        assert [len(c) for c in chunk_bytes(b"x" * 600_000)] == [262144, 262144, 75712]

    def test_small_payload_is_one_chunk(self):
        assert [len(c) for c in chunk_bytes(b"abc")] == [3]

    def test_empty_payload_yields_nothing(self):
        assert list(chunk_bytes(b"")) == []


class TestArrowEventRoundTrip:
    def test_round_trips(self):
        events = [
            DecodedEvent(timestamp=datetime(2026, 8, 1, 12, 0), event_code=1, event_param=2),
            DecodedEvent(timestamp=datetime(2026, 8, 1, 12, 1), event_code=8, event_param=3),
        ]
        back = arrow_batch_to_events(events_to_arrow_batch(events))
        assert [(e.event_code, e.event_param) for e in back] == [(1, 2), (8, 3)]
        assert back[0].timestamp == events[0].timestamp


class TestFoldResolution:
    """The host resolves the DST fall-back fold, not the decoder."""

    def test_backwards_jump_marks_the_fall_back_crossing(self):
        local = [
            datetime(2026, 11, 1, 1, 30),   # EDT, UTC-4
            datetime(2026, 11, 1, 1, 45),
            datetime(2026, 11, 1, 1, 30),   # clock went back -> EST, UTC-5
            datetime(2026, 11, 1, 1, 45),
        ]
        out = resolve_naive_local(local, NY)
        assert [u.hour for u in out] == [5, 5, 6, 6]
        assert all(u.tzinfo is not None for u in out)

    def test_ordinary_run_needs_no_fold(self):
        local = [datetime(2026, 8, 1, 12, 0), datetime(2026, 8, 1, 12, 1)]
        out = resolve_naive_local(local, NY)
        assert [u.hour for u in out] == [16, 16]   # EDT, UTC-4

    def test_result_is_utc(self):
        out = resolve_naive_local([datetime(2026, 8, 1, 12, 0)], NY)
        assert out[0].utcoffset().total_seconds() == 0
        assert out[0] == datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)

    def test_empty_run(self):
        assert resolve_naive_local([], NY) == []


class TestRemoteDecoderIsADecoder:
    @pytest.mark.asyncio
    async def test_describe_populates_the_in_process_attributes(self):
        decoder, conn = await _decoder()
        try:
            assert isinstance(decoder, BaseDecoder)
            assert decoder.name == "fake-asc3"
            assert decoder.extensions == [".dat"]
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_time_semantics_is_declared_not_applied(self):
        """A decoder declares its semantics; the host converts, not the decoder."""
        from tsigma.decoder.v1 import decoder_pb2

        decoder, conn = await _decoder("--naive-local")
        try:
            assert decoder.time_semantics == decoder_pb2.TIME_SEMANTICS_NAIVE_LOCAL
            out = await decoder.decode_remote(b"payload")
            # Timestamps come back exactly as emitted - naive, unconverted.
            assert out.result.events[0].timestamp == datetime(2026, 8, 1, 12, 0)
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_in_process_methods_refuse(self):
        decoder, conn = await _decoder()
        try:
            with pytest.raises(NotImplementedError):
                decoder.decode_bytes(b"x")
        finally:
            await conn.shutdown()


class TestCanDecode:
    @pytest.mark.asyncio
    async def test_true_for_a_supported_payload(self):
        decoder, conn = await _decoder()
        try:
            assert await decoder.can_decode_remote(b"\x01" * 100) is True
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_false_when_the_decoder_declines(self):
        decoder, conn = await _decoder("--cannot")
        try:
            assert await decoder.can_decode_remote(b"\x01" * 100) is False
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_sends_only_a_bounded_head(self):
        """A sniff must never ship the whole file."""
        decoder, conn = await _decoder()
        try:
            big = b"\x01" * (CAN_DECODE_HEAD_BYTES * 4)
            assert await decoder.can_decode_remote(big) is True
        finally:
            await conn.shutdown()


class TestDecodeOutcomes:
    @pytest.mark.asyncio
    async def test_success_returns_every_batch(self):
        decoder, conn = await _decoder()
        try:
            out = await decoder.decode_remote(b"payload")
            assert out.succeeded
            assert out.outcome == DecodeOutcome.SUCCESS
            assert len(out.result.events) == 3        # 2 batches + 1
            assert out.events_emitted == 3
            assert out.error == ""
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_partial_keeps_the_decodable_rows_and_reports_the_rest(self):
        decoder, conn = await _decoder("--partial")
        try:
            out = await decoder.decode_remote(b"payload")
            assert out.partial
            assert not out.failed
            assert len(out.result.events) == 2        # what survived
            assert out.segments_dropped == 1
            assert "truncated" in out.error
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_failure_yields_no_events_and_an_error(self):
        decoder, conn = await _decoder("--failure")
        try:
            out = await decoder.decode_remote(b"payload")
            assert out.failed
            assert out.result.events == []
            assert out.error == "unreadable header"
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_metadata_is_carried(self):
        decoder, conn = await _decoder("--with-metadata")
        try:
            out = await decoder.decode_remote(b"payload")
            assert out.result.metadata.device_ip == "10.0.0.1"
            assert out.result.metadata.log_version == "3.2"
            assert out.result.metadata.device_mac is None   # unset stays None
        finally:
            await conn.shutdown()


class TestMalformedStreams:
    @pytest.mark.asyncio
    async def test_missing_terminal_status_is_failure_not_success(self):
        """A crashed decoder cannot send a status; absence must never read as OK."""
        decoder, conn = await _decoder("--no-status")
        try:
            with pytest.raises(RemoteDecodeError, match="no terminal status"):
                await decoder.decode_remote(b"payload")
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_two_statuses_are_rejected(self):
        decoder, conn = await _decoder("--two-status")
        try:
            with pytest.raises(RemoteDecodeError, match="more than one terminal status"):
                await decoder.decode_remote(b"payload")
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_batch_after_the_status_is_rejected(self):
        decoder, conn = await _decoder("--batch-after")
        try:
            with pytest.raises(RemoteDecodeError, match="batch after the terminal status"):
                await decoder.decode_remote(b"payload")
        finally:
            await conn.shutdown()

    @pytest.mark.asyncio
    async def test_decoding_on_a_disconnected_plugin_is_an_error(self):
        decoder, conn = await _decoder()
        await conn.shutdown()
        with pytest.raises(RemoteDecodeError, match="not connected"):
            await decoder.decode_remote(b"payload")
