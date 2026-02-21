from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterable, List

import requests
from google.transit import gtfs_realtime_pb2  # from gtfs-realtime-bindings
from tenacity import retry, stop_after_attempt, wait_exponential

from platform.contracts.sncf_gtfsrt_schema import SncfRealtimeEvent
from platform.core.config import load_config
from platform.core.logger import setup_logging
from platform.ingestion.kafka_producer import KafkaConfig, KafkaJsonProducer


_LOGGER = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def _fetch_protobuf(url: str, headers: dict) -> bytes:
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.content


def _decode_feed(content: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    return feed


def _trip_update_events(feed: gtfs_realtime_pb2.FeedMessage, fetched_at_utc: str) -> List[SncfRealtimeEvent]:
    events: List[SncfRealtimeEvent] = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        # Best-effort extraction (varies by feed completeness)
        trip_id = tu.trip.trip_id if tu.trip and tu.trip.trip_id else None
        route_id = tu.trip.route_id if tu.trip and tu.trip.route_id else None

        # take first stop_time_update if exists
        stop_id = None
        delay_seconds = None
        if tu.stop_time_update:
            stu = tu.stop_time_update[0]
            stop_id = stu.stop_id or None
            if stu.arrival and stu.arrival.delay:
                delay_seconds = int(stu.arrival.delay)

        raw = {
            "trip_id": trip_id,
            "route_id": route_id,
            "stop_id": stop_id,
            "delay_seconds": delay_seconds,
        }

        events.append(
            SncfRealtimeEvent(
                event_type="TRIP_UPDATE",
                feed_timestamp_utc=fetched_at_utc,
                entity_id=entity.id,
                trip_id=trip_id,
                route_id=route_id,
                stop_id=stop_id,
                delay_seconds=delay_seconds,
                raw=raw,
            )
        )
    return events


def _service_alert_events(feed: gtfs_realtime_pb2.FeedMessage, fetched_at_utc: str) -> List[SncfRealtimeEvent]:
    events: List[SncfRealtimeEvent] = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        # cause/effect enums -> strings
        cause = str(alert.cause) if alert.HasField("cause") else None
        effect = str(alert.effect) if alert.HasField("effect") else None

        raw = {"cause": cause, "effect": effect}
        events.append(
            SncfRealtimeEvent(
                event_type="SERVICE_ALERT",
                feed_timestamp_utc=fetched_at_utc,
                entity_id=entity.id,
                cause=cause,
                effect=effect,
                raw=raw,
            )
        )
    return events


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> None:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    cfg = load_config()

    producer = KafkaJsonProducer(
        KafkaConfig(bootstrap_servers=cfg.kafka_bootstrap_servers, topic=cfg.kafka_topic_raw),
        logger=_LOGGER,
    )

    headers = {}
    # If SNCF requires API key header, set it via env
    api_key = os.getenv("SNCF_API_KEY", "").strip()
    if api_key:
        headers["Authorization"] = api_key  # adjust to required header if different

    _LOGGER.info("Starting SNCF GTFS-RT ingestion -> Kafka topic=%s", cfg.kafka_topic_raw)

    while True:
        fetched_at = _utc_now_iso()

        # Trip updates
        tu_bytes = _fetch_protobuf(cfg.sncf_trip_updates_url, headers=headers)
        tu_feed = _decode_feed(tu_bytes)
        tu_events = _trip_update_events(tu_feed, fetched_at)

        # Service alerts
        sa_bytes = _fetch_protobuf(cfg.sncf_service_alerts_url, headers=headers)
        sa_feed = _decode_feed(sa_bytes)
        sa_events = _service_alert_events(sa_feed, fetched_at)

        all_events: Iterable[SncfRealtimeEvent] = [*tu_events, *sa_events]
        count = 0
        for ev in all_events:
            # validate + to dict
            payload = ev.model_dump()
            producer.publish(key=ev.entity_id, event=payload)
            count += 1

        producer.flush()
        _LOGGER.info("Published %s events at %s", count, fetched_at)

        # SNCF refresh ~ 2 minutes, keep it aligned (you can set 60/120)
        time.sleep(int(os.getenv("POLL_SECONDS", "120")))


if __name__ == "__main__":
    main()
