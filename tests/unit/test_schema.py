from transport_platform.contracts.sncf_gtfsrt_schema import SncfRealtimeEvent


def test_event_model_valid() -> None:
    event = SncfRealtimeEvent(
        event_type="TRIP_UPDATE",
        feed_timestamp_utc="2026-01-01T00:00:00+00:00",
        entity_id="abc",
        raw={"k": "v"},
    )
    assert event.entity_id == "abc"
