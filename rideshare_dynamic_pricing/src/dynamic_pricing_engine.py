"""
fraud_detector.py -- PyFlink Fraud Detection Pipeline
======================================================
A real PyFlink streaming job that reads from Kafka, applies stateful fraud
detection rules, and writes alerts back to Kafka.

This job is submitted to a running Flink cluster via:
    flink run --python src/fraud_detector.py

Pipeline topology:

    KafkaSource("transactions")
        --> parse JSON
        --> assign timestamps + watermarks (event time)
        --> keyBy(customer_id)
            --> FraudDetectionFunction      (Rules R1, R2, R4, R5)  --+
            --> TumblingEventTimeWindows    (Rule R3)                 --+--> union --> KafkaSink("fraud-alerts")

Concepts demonstrated
---------------------
- KafkaSource: unbounded stream source (replaces FileSource in production)
- Event time + bounded out-of-orderness watermarks
- KeyedProcessFunction: stateful per-customer fraud rules
- ValueState: stores last transaction amount per customer (Rule R5)
- ListState: stores recent transaction history per customer (Rule R2)
- StateTtlConfig: automatic state expiry after 1 hour of inactivity
- TumblingEventTimeWindows: aggregate rule over a fixed time bucket (Rule R3)
- Multi-stream union: merge alert streams from all rules
- KafkaSink: writes alerts to "fraud-alerts" topic for the dashboard

Configuration
-------------
KAFKA_BOOTSTRAP  Kafka broker address (default: kafka:9092)
TRANSACTIONS_TOPIC   Input topic  (default: transactions)
ALERTS_TOPIC         Output topic (default: fraud-alerts)
"""

import json
import os
from datetime import datetime

# -- PyFlink imports ----------------------------------------------------------
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    MapFunction,
    KeyedProcessFunction,
    RuntimeContext,
    ProcessWindowFunction,
)
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
    StateTtlConfig,
)
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common import Time, Types, WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP",    "kafka:9092")
RIDES_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "ride-events")
ALERTS_TOPIC       = os.getenv("ALERTS_TOPIC",       "pricing-alerts")


WINDOW_SIZE_MINUTES  = 5   # recent-history window size
WINDOW_SLIDE_MINUTES = 1   # update every minute for faster updates

ALLOWED_LATENESS_SECONDS = 19  # keep window open for slightly delayed records
WATERMARK_DELAY_SECONDS  = 5   # assume records may arrive out of order

DEMAND_RATIO_THRESHOLD = 1.8   # high rider-to-driver pressure threshold
AVG_COST_THRESHOLD     = 30.0  # simple baseline threshold for high average ride cost


# -----------------------------------------------------------------------------
# SECTION 1: DATA MODEL
# -----------------------------------------------------------------------------

class RideEvent:
    """Plain Python object representing one parsed rideshare event"""
    __slots__ = [
        "ride_id",
        "location_category",
        "vehicle_type",
        "time_of_booking",
        "customer_loyalty_status",
        "number_of_riders",
        "number_of_drivers",
        "number_of_past_rides",
        "average_ratings",
        "expected_ride_duration",
        "historical_cost_of_ride",
        "event_time",
    ]

    def __init__(self, d: dict):
        self.ride_id                 = d["ride_id"]
        self.location_category       = d["location_category"]
        self.vehicle_type            = d["vehicle_type"]
        self.time_of_booking         = d.get("time_of_booking", "")
        self.customer_loyalty_status = d.get("customer_loyalty_status", "")
        self.number_of_riders        = int(d["number_of_riders"])
        self.number_of_drivers       = int(d["number_of_drivers"])
        self.number_of_past_rides    = int(d.get("number_of_past_rides", 0))
        self.average_ratings         = float(d.get("average_ratings", 0.0))
        self.expected_ride_duration  = int(d.get("expected_ride_duration", 0))
        self.historical_cost_of_ride = float(d["historical_cost_of_ride"])
        self.event_time              = int(d["event_time"])  # unix ms

class PricingAlert:
    """A pricing anomaly alert emitted by the detection pipeline."""

    def __init__(self, segment_key: str, ride_count: int,
                 avg_cost: float, avg_demand_ratio: float,
                 max_cost: float, window_start: int, window_end: int,
                 alert_type: str, details: str):
        self.segment_key       = segment_key
        self.ride_count        = ride_count
        self.avg_cost          = round(avg_cost, 2)
        self.avg_demand_ratio  = round(avg_demand_ratio, 2)
        self.max_cost          = round(max_cost, 2)
        self.window_start      = window_start
        self.window_end        = window_end
        self.alert_type        = alert_type
        self.details           = details
        self.alert_time        = int(datetime.now().timestamp() * 1000)

    def to_json(self) -> str:
        return json.dumps({
            "segment_key":       self.segment_key,
            "ride_count":        self.ride_count,
            "avg_cost":          self.avg_cost,
            "avg_demand_ratio":  self.avg_demand_ratio,
            "max_cost":          self.max_cost,
            "window_start":      self.window_start,
            "window_end":        self.window_end,
            "alert_type":        self.alert_type,
            "details":           self.details,
            "alert_time":        self.alert_time,
        })


# -----------------------------------------------------------------------------
# SECTION 2: PARSING
# -----------------------------------------------------------------------------

class ParseRideEvent(MapFunction):
    """
    MapFunction: raw Kafka message (JSON string) --> validated JSON string.

    Malformed or incomplete messages are dropped (return None) and filtered
    downstream. In production, route them to a dead-letter topic instead.
    """

    def map(self, raw: str) -> str:
        try:
            d = json.loads(raw.strip())
            if not all(k in d for k in (
                "ride_id", "location_category", "vehicle_type", "number_of_riders", 
                "number_of_drivers", "historical_cost_of_ride", "event_time",)):
                return None
            return json.dumps(d)
        except (json.JSONDecodeError, KeyError, ValueError):
            return None


# -----------------------------------------------------------------------------
# SECTION 3: WATERMARK STRATEGY
# -----------------------------------------------------------------------------

class RideTimestampAssigner(TimestampAssigner):
    """
    Extracts event_time from each rideshare event for Flink's event-time clock.

    Watermark formula: watermark = max(event_time_seen) - 5000ms
    This means:
      - Events arriving up to 5 seconds late are still processed
      - Events arriving more than 5 seconds late are treated as late data
      - Windows fire when the watermark passes their end boundary
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["event_time"])
        except Exception:
            return record_timestamp


# Windowed Aggregation
class PricingWindowFunction(ProcessWindowFunction): # computes pricing metrics over each sliding event-time window
                                                    # results in overlapping events
    def process(self, key: str,
                context: ProcessWindowFunction.Context,
                elements) -> list:

        ride_count = 0           # number of rides in the current window
        total_cost = 0.0         # sum of ride costs across the window
        total_demand_ratio = 0.0 # sum of riders/drivers ratios across the window
        max_cost = 0.0           # highest single ride cost seen in the window

        # loop through each record assigned to this window
        for elem in elements:
            try:
                d = json.loads(elem)
                ride = RideEvent(d) # convert to RideEvent object
                ride_count += 1

                total_cost += ride.historical_cost_of_ride # add ride's historical cost to the running total

                drivers = max(ride.number_of_drivers, 1) # avoid divide-by-zero if drivers is ever 0
                demand_ratio = ride.number_of_riders / drivers
                total_demand_ratio += demand_ratio # add ratio to running total
                if ride.historical_cost_of_ride > max_cost:
                    max_cost = ride.historical_cost_of_ride # track highest cost in window

            except Exception:
                pass

        # if all rides in window are invalid, emit nothing
        if ride_count == 0:
            return []

        # compute average metrics after processing all rides
        avg_cost = total_cost / ride_count
        avg_demand_ratio = total_demand_ratio / ride_count

        # None unless one or more anomaly rules fires
        alert_type = None
        details = None

        # Rule P1: if average rider-to-driver ratio is high for the window, flag key as demand pressure
        if avg_demand_ratio >= DEMAND_RATIO_THRESHOLD:
            alert_type = "HIGH_DEMAND_PRESSURE"
            details = (
                f"Average demand ratio is {avg_demand_ratio:.2f} "
                f"across {ride_count} rides in the last {WINDOW_SIZE_MINUTES} minutes"
            )

        # Rule P2: if average ride cost is high for the window, flag key as unusually high recent pricing
        elif avg_cost >= AVG_COST_THRESHOLD:
            alert_type = "HIGH_AVERAGE_COST"
            details = (
                f"Average ride cost is ${avg_cost:,.2f} "
                f"across {ride_count} rides in the last {WINDOW_SIZE_MINUTES} minutes"
            )

        # emit an alert only if at least one rule fired
        if alert_type is not None:
            alert = PricingAlert(
                segment_key=key,                     # current  segment
                ride_count=ride_count,               # number of rides in window
                avg_cost=avg_cost,                   # average cost in window
                avg_demand_ratio=avg_demand_ratio,   # average demand pressure in window
                max_cost=max_cost,                   # highest ride cost in window
                window_start=context.window().start, # window start time in unix ms
                window_end=context.window().end,     # window end time in unix ms
                alert_type=alert_type,               # anomaly rule fired
                details=details,                     # explanation
            )
            return [alert.to_json()]

        # if no anomaly rule fired, emit nothing
        return []
    
# Pipeline Assembly
def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # simpler debugging in developing

    env.enable_checkpointing(30_000) # checkpoint every 30 seconds

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)   # broker address
        .set_topics(RIDES_TOPIC)                  # topic name
        .set_group_id("pricing-detector-group")   # consumer group for this job
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) # only process new records arriving after the job starts
        .set_value_only_deserializer(SimpleStringSchema()) # read Kafka values as plain strings
        .build()
    )

    # create a DataStream from Kafka
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),   # watermarks applied after parsing
        "KafkaSource[ride-events]"           # operator name
    )

    # parse raw strings into validated JSON strings
    parsed_stream = (
        raw_stream
        .map(ParseRideEvent(), output_type=Types.STRING())
        .filter(lambda x: x is not None)     # drop malformed records
    )

    # assign timestamps and watermarks + switch into event-time processing after parsing
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_DELAY_SECONDS))
        .with_timestamp_assigner(RideTimestampAssigner())
        # extract event_time from each JSON record
    )
    timed_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    # KeyBy segment - group records by pricing segment: location_category|vehicle_type
    keyed_stream = timed_stream.key_by(
        lambda x: (
            f"{json.loads(x)['location_category']}|"
            f"{json.loads(x)['vehicle_type']}"
        ),
        key_type=Types.STRING()
    )

    # sliding event-time window
    # each event may belong to multiple overlapping windows
    alert_stream = (
        keyed_stream
        .window(
            SlidingEventTimeWindows.of(
                Time.minutes(WINDOW_SIZE_MINUTES),   # fixed length
                Time.minutes(WINDOW_SLIDE_MINUTES),  # slide
            )
        )
        .allowed_lateness(Time.seconds(ALLOWED_LATENESS_SECONDS)) # slightly late events can still count
        .process(PricingWindowFunction(), output_type=Types.STRING()) # for each window, compute metrics and emit alert if anomalous
    )

    # Sink: Write output alerts to the pricing-alerts Kafka topic.
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ALERTS_TOPIC) # output topic name
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alert_stream.sink_to(sink) # connect alert stream to Kafka sink

    alert_stream.print() # print alerts for terminal logs

    print("Submitting Flink job: Dynamic Pricing Via Anomaly Detection Pipeline")
    print(f"  Kafka source: {KAFKA_BOOTSTRAP} / {RIDES_TOPIC}")
    print(f"  Kafka sink:   {KAFKA_BOOTSTRAP} / {ALERTS_TOPIC}")
    env.execute("Dynamic Pricing Via Anomaly Detection Pipeline")  # launch Flink job


if __name__ == "__main__":
    print("=" * 70)
    print("  PyFlink Dynamic Pricing / Anomaly Detection Pipeline")
    print("=" * 70)
    print()
    print("Concepts demonstrated:")
    print("  - KafkaSource: unbounded streaming input")
    print("  - Event time + bounded out-of-orderness watermarks")
    print("  - SlidingEventTimeWindows: recent-history aggregation")
    print("  - allowed_lateness: brief tolerance for delayed events")
    print("  - KafkaSink: streaming output to pricing-alerts")
    print("  - Checkpointing: fault tolerance (30s interval)")
    print()

    build_pipeline()