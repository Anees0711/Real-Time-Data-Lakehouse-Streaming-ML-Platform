from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


EventType = Literal["TRIP_UPDATE", "SERVICE_ALERT"]


class SncfRealtimeEvent(BaseModel):
    """Normalized event shape stored in Kafka and lakehouse."""

    event_type: EventType
    feed_timestamp_utc: str = Field(..., description="ISO8601 UTC timestamp of feed fetch time")
    entity_id: str = Field(..., description="GTFS-RT entity ID")
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    stop_id: Optional[str] = None
    delay_seconds: Optional[int] = None
    cause: Optional[str] = None
    effect: Optional[str] = None
    raw: dict = Field(..., description="Original decoded entity payload")
