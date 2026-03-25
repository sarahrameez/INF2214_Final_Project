from __future__ import annotations

import argparse
import errno
import json
import socket
import sys
import time
from datetime import datetime, timedelta, timezone


SURGE_ZONE = "Downtown"
ALL_ZONES = [SURGE_ZONE, "Airport", "Suburb_North", "University_Campus"]
BASE_EVENTS_PER_TICK = 1
DEFAULT_TICK_SECONDS = 0.25
SURGE_CYCLE_SECONDS = 45
ZONE_PULSE_SCHEDULE = {
    "Downtown": ((18, 22, 8), (31, 35, 7)),
    "Airport": ((24, 28, 6),),
    "Suburb_North": ((28, 31, 5),),
    "University_Campus": ((36, 40, 6),),
}
DEFAULT_START_TIME = datetime(2026, 3, 23, 18, 0, 0, tzinfo=timezone.utc)
OUT_OF_ORDER_INTERVAL_SECONDS = 10
OUT_OF_ORDER_DELAY_SECONDS = 3
VERY_LATE_INTERVAL_SECONDS = 20
VERY_LATE_DELAY_SECONDS = 12


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deterministic ride-request generator for PyFlink surge detection demos."
    )
    parser.add_argument(
        "--mode",
        choices=["socket-server", "stdout"],
        default="socket-server",
        help="Emit events to stdout or serve them over a TCP socket.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host for socket mode.")
    parser.add_argument("--port", type=int, default=9999, help="Bind port for socket mode.")
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=0,
        help="Stop after this many seconds. Zero means run forever.",
    )
    parser.add_argument(
        "--start-time",
        default=DEFAULT_START_TIME.isoformat().replace("+00:00", "Z"),
        help="UTC ISO-8601 timestamp used as the logical stream start time.",
    )
    parser.add_argument(
        "--tick-seconds",
        type=float,
        default=DEFAULT_TICK_SECONDS,
        help="Wall-clock and logical spacing between generator ticks in seconds.",
    )
    parser.add_argument(
        "--inject-delays",
        action="store_true",
        help="Inject deterministic out-of-order and late Downtown events for delay handling demos.",
    )
    return parser


def parse_start_time(start_time: str) -> datetime:
    normalized = start_time.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_interval_ticks(interval_seconds: int, tick_seconds: float) -> int:
    return max(1, int(round(interval_seconds / tick_seconds)))


def pulse_extra_events(zone_id: str, logical_elapsed_seconds: float) -> int:
    cycle_position = logical_elapsed_seconds % SURGE_CYCLE_SECONDS
    extra_events = 0
    for start_second, end_second, extra_per_tick in ZONE_PULSE_SCHEDULE.get(zone_id, ()):
        if start_second <= cycle_position < end_second:
            extra_events += extra_per_tick
    return extra_events


def build_delay_seconds(
    tick_index: int,
    zone_id: str,
    event_index: int,
    inject_delays: bool,
    tick_seconds: float,
) -> int:
    if (
        not inject_delays
        or zone_id != SURGE_ZONE
        or event_index != 0
        or pulse_extra_events(zone_id, tick_index * tick_seconds) > 0
    ):
        return 0

    very_late_interval_ticks = build_interval_ticks(
        VERY_LATE_INTERVAL_SECONDS, tick_seconds
    )
    out_of_order_interval_ticks = build_interval_ticks(
        OUT_OF_ORDER_INTERVAL_SECONDS, tick_seconds
    )
    if tick_index and tick_index % very_late_interval_ticks == 0:
        return VERY_LATE_DELAY_SECONDS
    if tick_index and tick_index % out_of_order_interval_ticks == 0:
        return OUT_OF_ORDER_DELAY_SECONDS
    return 0


def build_ride_id(tick_index: int, zone_id: str, event_index: int) -> str:
    return f"ride-{tick_index:07d}-{zone_id.lower()}-{event_index:02d}"


def build_event(
    tick_index: int,
    logical_now: datetime,
    zone_id: str,
    event_index: int,
    inject_delays: bool,
    tick_seconds: float,
) -> dict:
    delay_seconds = build_delay_seconds(
        tick_index,
        zone_id,
        event_index,
        inject_delays=inject_delays,
        tick_seconds=tick_seconds,
    )
    event_time = logical_now - timedelta(seconds=delay_seconds)
    return {
        "ride_id": build_ride_id(tick_index, zone_id, event_index),
        "zone_id": zone_id,
        "timestamp": event_time.isoformat().replace("+00:00", "Z"),
        "is_simulated_late": delay_seconds > 0,
    }


def build_tick_events(
    tick_index: int,
    logical_now: datetime,
    inject_delays: bool,
    tick_seconds: float,
) -> list[dict]:
    logical_elapsed_seconds = tick_index * tick_seconds
    events: list[dict] = []

    for zone_id in ALL_ZONES:
        total_events = BASE_EVENTS_PER_TICK + pulse_extra_events(
            zone_id, logical_elapsed_seconds
        )
        for event_index in range(total_events):
            events.append(
                build_event(
                    tick_index,
                    logical_now,
                    zone_id,
                    event_index=event_index,
                    inject_delays=inject_delays,
                    tick_seconds=tick_seconds,
                )
            )

    return events


def event_stream(args: argparse.Namespace):
    logical_start_time = parse_start_time(args.start_time)
    tick_index = 0

    while True:
        logical_elapsed_seconds = tick_index * args.tick_seconds
        if args.max_seconds and logical_elapsed_seconds >= args.max_seconds:
            break

        logical_now = logical_start_time + timedelta(seconds=logical_elapsed_seconds)
        for event in build_tick_events(
            tick_index,
            logical_now,
            inject_delays=args.inject_delays,
            tick_seconds=args.tick_seconds,
        ):
            yield json.dumps(event, separators=(",", ":"))

        tick_index += 1
        time.sleep(args.tick_seconds)


def run_stdout(args: argparse.Namespace) -> None:
    for raw_event in event_stream(args):
        print(raw_event)
        sys.stdout.flush()


def run_socket_server(args: argparse.Namespace) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((args.host, args.port))
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                raise SystemExit(
                    f"Port {args.port} is already in use on {args.host}. "
                    "Stop the existing listener or rerun both the generator and "
                    "the Flink job on a different port, for example --port 9998."
                ) from exc
            raise

        server.listen(1)
        print(
            f"Listening for Flink socket source on {args.host}:{args.port}",
            file=sys.stderr,
        )
        connection, client_address = server.accept()
        with connection:
            print(f"Client connected from {client_address}", file=sys.stderr)
            try:
                for raw_event in event_stream(args):
                    connection.sendall((raw_event + "\n").encode("utf-8"))
            except BrokenPipeError:
                print("Client disconnected.", file=sys.stderr)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.max_seconds < 0:
        raise ValueError("--max-seconds must be non-negative.")
    if args.tick_seconds <= 0:
        raise ValueError("--tick-seconds must be greater than 0.")
    parse_start_time(args.start_time)
    if args.mode == "stdout":
        run_stdout(args)
    else:
        run_socket_server(args)


if __name__ == "__main__":
    main()
