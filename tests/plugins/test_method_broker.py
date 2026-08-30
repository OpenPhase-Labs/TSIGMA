"""P8: host-served method broker services.

A method plugin is transport-only. These are the only ways it reaches host state,
and the boundary is where an untrusted plugin must not be able to overreach.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import tsigma.plugins  # noqa: F401  - puts the generated stubs on sys.path
from tsigma.collection.ingest import IngestOutcome, IngestResult
from tsigma.plugins.method_broker import (
    ERROR_MSG_MAX,
    CheckpointService,
    EventSinkService,
    collect_chunks,
    device_type_of,
    to_persist_response,
)
from tsigma.method.v1 import method_pb2

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


async def _frames(*items):
    for item in items:
        yield item


class TestDeviceType:
    def test_maps_the_contract_enum(self):
        assert device_type_of(method_pb2.DEVICE_TYPE_CONTROLLER) == "controller"
        assert device_type_of(method_pb2.DEVICE_TYPE_SENSOR) == "sensor"

    def test_unspecified_defaults_to_controller(self):
        assert device_type_of(method_pb2.DEVICE_TYPE_UNSPECIFIED) == "controller"


class TestCheckpointService:
    @pytest.mark.asyncio
    async def test_load_forwards_to_the_sdk(self):
        req = method_pb2.LoadCheckpointRequest(
            method_name="ftp_pull", device_id="SIG-1",
            device_type=method_pb2.DEVICE_TYPE_CONTROLLER,
        )
        with patch("tsigma.collection.sdk.load_checkpoint",
                   new_callable=AsyncMock, return_value=None) as m:
            await CheckpointService(AsyncMock()).load_checkpoint(req)
        assert m.call_args[0][:3] == ("ftp_pull", "controller", "SIG-1")

    @pytest.mark.asyncio
    async def test_save_forwards_only_the_fields_that_were_set(self):
        """Absence means leave unchanged - an unset field must not clear a column."""
        req = method_pb2.SaveCheckpointRequest(
            method_name="ftp_pull", device_id="SIG-1", last_filename="events.dat",
        )
        with patch("tsigma.collection.sdk.save_checkpoint", new_callable=AsyncMock) as m:
            await CheckpointService(AsyncMock()).save_checkpoint(req)
        kwargs = m.call_args[1]
        assert kwargs == {"last_filename": "events.dat"}
        assert "files_hash" not in kwargs
        assert "last_event_timestamp" not in kwargs

    @pytest.mark.asyncio
    async def test_save_forwards_timestamps_when_present(self):
        req = method_pb2.SaveCheckpointRequest(method_name="m", device_id="d")
        req.last_event_timestamp.FromDatetime(NOW)
        with patch("tsigma.collection.sdk.save_checkpoint", new_callable=AsyncMock) as m:
            await CheckpointService(AsyncMock()).save_checkpoint(req)
        assert "last_event_timestamp" in m.call_args[1]

    @pytest.mark.asyncio
    async def test_zero_counters_are_a_no_op(self):
        """0 adds nothing, so it must not be forwarded as an update."""
        req = method_pb2.SaveCheckpointRequest(
            method_name="m", device_id="d", events_ingested=0,
        )
        with patch("tsigma.collection.sdk.save_checkpoint", new_callable=AsyncMock) as m:
            await CheckpointService(AsyncMock()).save_checkpoint(req)
        assert "events_ingested" not in m.call_args[1]

    @pytest.mark.asyncio
    async def test_counters_are_forwarded_when_nonzero(self):
        req = method_pb2.SaveCheckpointRequest(
            method_name="m", device_id="d", events_ingested=7, duplicates_absorbed=2,
        )
        with patch("tsigma.collection.sdk.save_checkpoint", new_callable=AsyncMock) as m:
            await CheckpointService(AsyncMock()).save_checkpoint(req)
        assert m.call_args[1]["events_ingested"] == 7
        assert m.call_args[1]["duplicates_absorbed"] == 2

    @pytest.mark.asyncio
    async def test_record_error_truncates(self):
        """The contract has the host truncate; a plugin cannot write unbounded text."""
        req = method_pb2.RecordErrorRequest(
            method_name="m", device_id="d", error_msg="x" * 5000,
        )
        with patch("tsigma.collection.sdk.record_error", new_callable=AsyncMock) as m:
            await CheckpointService(AsyncMock()).record_error(req)
        assert len(m.call_args[0][4]) == ERROR_MSG_MAX


class TestPersistResponse:
    def test_carries_outcome_and_high_water_mark(self):
        out = to_persist_response(
            IngestResult(IngestOutcome.PARTIAL, 3, NOW, events_decoded=9)
        )
        assert out.events_inserted == 3
        assert out.outcome == method_pb2.INGEST_OUTCOME_PARTIAL
        assert out.HasField("max_event_time")

    def test_no_mark_leaves_the_timestamp_unset(self):
        out = to_persist_response(IngestResult(IngestOutcome.FAILURE, 0, None))
        assert out.HasField("max_event_time") is False
        assert out.outcome == method_pb2.INGEST_OUTCOME_FAILURE

    def test_duplicates_are_not_on_the_wire(self):
        """The host computes attempted-minus-inserted; the plugin never asserts it."""
        assert "duplicates_absorbed" not in {
            f.name for f in method_pb2.PersistResponse.DESCRIPTOR.fields
        }


class TestChunkReassembly:
    @pytest.mark.asyncio
    async def test_reassembles_header_then_chunks(self):
        header = method_pb2.DecodeAndPersistHeader(
            device_id="SIG-1", decoder_name="asc3",
        )
        frames = [
            method_pb2.DecodeAndPersistRequest(header=header),
            method_pb2.DecodeAndPersistRequest(chunk=b"abc"),
            method_pb2.DecodeAndPersistRequest(chunk=b"def"),
        ]
        got_header, payload = await collect_chunks(_frames(*frames))
        assert got_header.device_id == "SIG-1"
        assert payload == b"abcdef"

    @pytest.mark.asyncio
    async def test_a_chunk_before_the_header_is_refused(self):
        frames = [method_pb2.DecodeAndPersistRequest(chunk=b"abc")]
        with pytest.raises(ValueError, match="chunk before the header"):
            await collect_chunks(_frames(*frames))

    @pytest.mark.asyncio
    async def test_a_second_header_is_refused(self):
        h = method_pb2.DecodeAndPersistHeader(device_id="S")
        frames = [
            method_pb2.DecodeAndPersistRequest(header=h),
            method_pb2.DecodeAndPersistRequest(header=h),
        ]
        with pytest.raises(ValueError, match="more than one header"):
            await collect_chunks(_frames(*frames))

    @pytest.mark.asyncio
    async def test_an_empty_stream_is_refused(self):
        with pytest.raises(ValueError, match="no header frame"):
            await collect_chunks(_frames())


class TestEventSink:
    @pytest.mark.asyncio
    async def test_runs_the_payload_through_ingest_raw(self):
        header = method_pb2.DecodeAndPersistHeader(
            device_id="SIG-1", decoder_name="asc3", source_label="signal",
        )
        with patch("tsigma.plugins.method_broker.ingest_raw",
                   new_callable=AsyncMock,
                   return_value=IngestResult(IngestOutcome.SUCCESS, 4, NOW,
                                             events_decoded=4)) as ingest:
            out = await EventSinkService(AsyncMock()).decode_and_persist(header, b"raw")
        assert ingest.call_args[0][0] == b"raw"
        assert ingest.call_args[1]["device_id"] == "SIG-1"
        assert ingest.call_args[1]["decoder_name"] == "asc3"
        assert out.events_inserted == 4
        assert out.outcome == method_pb2.INGEST_OUTCOME_SUCCESS

    @pytest.mark.asyncio
    async def test_a_failed_ingest_crosses_as_an_outcome(self):
        header = method_pb2.DecodeAndPersistHeader(device_id="S", decoder_name="d")
        with patch("tsigma.plugins.method_broker.ingest_raw",
                   new_callable=AsyncMock,
                   return_value=IngestResult(IngestOutcome.FAILURE, 0, None,
                                             error="bad")):
            out = await EventSinkService(AsyncMock()).decode_and_persist(header, b"x")
        assert out.outcome == method_pb2.INGEST_OUTCOME_FAILURE
        assert out.events_inserted == 0
