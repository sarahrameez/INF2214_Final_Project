from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

RIDE_REQUEST_INDEX = {
    "ride_id": 0,
    "zone_id": 1,
    "timestamp": 2,
    "event_time_millis": 3,
    "is_simulated_late": 4,
}


def parse_iso8601_to_millis(timestamp: str) -> int:
    normalized = timestamp.replace("Z", "+00:00")
    event_time = datetime.fromisoformat(normalized)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return int(event_time.timestamp() * 1000)


def millis_to_iso8601(timestamp_millis: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_millis / 1000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


@dataclass
class RideRequest:
    ride_id: str
    zone_id: str
    timestamp: str
    event_time_millis: int
    is_simulated_late: bool = False

    @classmethod
    def from_json(cls, raw_event: str) -> "RideRequest":
        payload = json.loads(raw_event)
        required_fields = {"ride_id", "zone_id", "timestamp"}
        missing = required_fields.difference(payload)
        if missing:
            missing_fields = ", ".join(sorted(missing))
            raise ValueError(f"Missing required event fields: {missing_fields}")

        event_time_millis = payload.get("event_time_millis")
        if event_time_millis is None:
            event_time_millis = parse_iso8601_to_millis(payload["timestamp"])

        return cls(
            ride_id=str(payload["ride_id"]),
            zone_id=str(payload["zone_id"]),
            timestamp=str(payload["timestamp"]),
            event_time_millis=int(event_time_millis),
            is_simulated_late=bool(payload.get("is_simulated_late", False)),
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "ride_id": self.ride_id,
                "zone_id": self.zone_id,
                "timestamp": self.timestamp,
                "event_time_millis": self.event_time_millis,
                "is_simulated_late": self.is_simulated_late,
            },
            separators=(",", ":"),
            sort_keys=True,
        )


def parse_ride_request(raw_event: str) -> tuple[str, str, str, int, bool]:
    event = RideRequest.from_json(raw_event)
    return (
        event.ride_id,
        event.zone_id,
        event.timestamp,
        event.event_time_millis,
        event.is_simulated_late,
    )


def extract_zone_id(event: tuple[str, str, str, int, bool]) -> str:
    return event[RIDE_REQUEST_INDEX["zone_id"]]


def extract_event_time_millis(event: tuple[str, str, str, int, bool]) -> int:
    return event[RIDE_REQUEST_INDEX["event_time_millis"]]


def format_alert(
    zone_id: str,
    window_end_millis: int,
    current_count: int,
    baseline: float,
    status: str,
) -> str:
    payload = {
        "timestamp": millis_to_iso8601(window_end_millis),
        "zone": zone_id,
        "request_count": int(current_count),
        "ema_baseline": round(float(baseline), 2),
        "status": status,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def format_late_event(event: tuple[str, str, str, int, bool]) -> str:
    return (
        "LATE_EVENT | "
        f"zone={event[RIDE_REQUEST_INDEX['zone_id']]} | "
        f"ride_id={event[RIDE_REQUEST_INDEX['ride_id']]} | "
        f"timestamp={event[RIDE_REQUEST_INDEX['timestamp']]} | "
        f"is_simulated_late={str(event[RIDE_REQUEST_INDEX['is_simulated_late']]).lower()}"
    )
