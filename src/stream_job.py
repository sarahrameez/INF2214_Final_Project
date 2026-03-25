from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyflink.common import Types
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import SourceFunction
from pyflink.java_gateway import get_gateway

try:
    from .config import (
        ALLOWED_LATENESS_SECONDS,
        EMA_ALPHA,
        OUTPUT_FILE,
        SURGE_THRESHOLD,
        WATERMARK_DELAY_SECONDS,
        WINDOW_SECONDS,
        WINDOW_SLIDE_SECONDS,
    )
    from .schema import (
        extract_event_time_millis,
        extract_zone_id,
        parse_ride_request,
    )
    from .surge_detector import SurgeDetector
except ImportError:
    from config import (
        ALLOWED_LATENESS_SECONDS,
        EMA_ALPHA,
        OUTPUT_FILE,
        SURGE_THRESHOLD,
        WATERMARK_DELAY_SECONDS,
        WINDOW_SECONDS,
        WINDOW_SLIDE_SECONDS,
    )
    from schema import (
        extract_event_time_millis,
        extract_zone_id,
        parse_ride_request,
    )
    from surge_detector import SurgeDetector


RIDE_REQUEST_TYPE = Types.TUPLE(
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.BOOLEAN(),
    ]
)


class RideRequestTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return extract_event_time_millis(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Real-time ride-hailing demand surge detection with PyFlink."
    )
    parser.add_argument("--host", default="localhost", help="Socket source host.")
    parser.add_argument(
        "--port",
        type=int,
        default=9999,
        help="Socket source port exposed by the generator.",
    )
    parser.add_argument(
        "--window-seconds",
        type=int,
        default=WINDOW_SECONDS,
        help="Event-time window length in seconds.",
    )
    parser.add_argument(
        "--slide-seconds",
        type=int,
        default=WINDOW_SLIDE_SECONDS,
        help="Event-time window slide in seconds.",
    )
    parser.add_argument(
        "--watermark-seconds",
        type=int,
        default=WATERMARK_DELAY_SECONDS,
        help="Bounded out-of-orderness watermark delay in seconds.",
    )
    parser.add_argument(
        "--allowed-lateness-seconds",
        type=int,
        default=ALLOWED_LATENESS_SECONDS,
        help="Allowed lateness for late-but-still-counted events in seconds.",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=EMA_ALPHA,
        help="EMA weighting factor for the latest window count.",
    )
    parser.add_argument(
        "--surge-threshold",
        type=float,
        default=SURGE_THRESHOLD,
        help="Multiplier applied to the EMA baseline to flag surges.",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Job parallelism for local execution.",
    )
    parser.add_argument(
        "--output-file",
        default=str(OUTPUT_FILE),
        help="Path to the JSONL file that receives alert output.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.port <= 0:
        raise ValueError("--port must be greater than 0.")
    if args.window_seconds <= 0:
        raise ValueError("--window-seconds must be greater than 0.")
    if args.slide_seconds <= 0:
        raise ValueError("--slide-seconds must be greater than 0.")
    if args.slide_seconds > args.window_seconds:
        raise ValueError("--slide-seconds must be less than or equal to --window-seconds.")
    if args.watermark_seconds < 0:
        raise ValueError("--watermark-seconds must be non-negative.")
    if args.allowed_lateness_seconds < 0:
        raise ValueError("--allowed-lateness-seconds must be non-negative.")
    if not 0 < args.alpha <= 1:
        raise ValueError("--alpha must be in the range (0, 1].")
    if args.surge_threshold <= 0:
        raise ValueError("--surge-threshold must be greater than 0.")
    if args.parallelism <= 0:
        raise ValueError("--parallelism must be greater than 0.")


def build_socket_source(
    env: StreamExecutionEnvironment, host: str, port: int
) -> DataStream:
    j_socket_source = (
        get_gateway()
        .jvm.org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction(
            host,
            port,
            "\n",
            0,
        )
    )
    return env.add_source(
        SourceFunction(j_socket_source),
        source_name="Socket Text Stream",
        type_info=Types.STRING(),
    )


def build_job(args: argparse.Namespace) -> DataStream:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    env.set_python_executable(sys.executable)

    source_stream = build_socket_source(env, args.host, args.port)

    ride_request_stream = (
        source_stream.filter(lambda raw: bool(raw and raw.strip()))
        .map(parse_ride_request, output_type=RIDE_REQUEST_TYPE)
        .name("Parse Ride Requests")
    )

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(args.watermark_seconds)
    ).with_timestamp_assigner(RideRequestTimestampAssigner())

    timed_stream = ride_request_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    alerts = timed_stream.key_by(
        extract_zone_id, key_type=Types.STRING()
    ).process(
        SurgeDetector(
            alpha=args.alpha,
            surge_threshold=args.surge_threshold,
            window_seconds=args.window_seconds,
            slide_seconds=args.slide_seconds,
            allowed_lateness_seconds=args.allowed_lateness_seconds,
        ),
        output_type=Types.STRING(),
    ).name("Detect Surges")

    return alerts


def stream_results_to_file(alerts: DataStream, alert_output_file: Path) -> None:
    alert_output_file.parent.mkdir(parents=True, exist_ok=True)
    alert_output_file.write_text("", encoding="utf-8")

    with alert_output_file.open("a", encoding="utf-8") as alert_sink:
        with alerts.execute_and_collect(
            "Ride-Hailing Demand Surge Detection"
        ) as results:
            for alert in results:
                alert_sink.write(f"{alert}\n")
                alert_sink.flush()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)
    alerts = build_job(args)
    stream_results_to_file(
        alerts=alerts,
        alert_output_file=Path(args.output_file).expanduser().resolve(),
    )


if __name__ == "__main__":
    main()
