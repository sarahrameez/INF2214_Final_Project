# AI was used to brainstorm ideas on how to loop through the finite data source to create streaming data.
# Tool Used: ChatGPT 5.2
# Date Used: 21-03-2026
# Summary of use: We used AI to figure out a way to continuously loop through the .csv data and point to documentation
# to help us implement it.

"""
data_generator.py -- Simulated Rideshare Event Stream (Kafka Producer)
======================================================================
Reads rideshare pricing records from a CSV file and replays them as a
continuous Kafka stream to the topic "ride-events".

Each message is a JSON-encoded rideshare event. The downstream PyFlink pricing /
anomaly detection job reads from this topic via KafkaSource and computes
windowed pricing metrics and anomaly alerts in real time.

Each event is assigned an `event_time` in Unix milliseconds for Flink
event-time processing. The generator also introduces occasional out-of-order
and late records so the streaming job can demonstrate watermarking and
late-data handling.

Source: https://www.kaggle.com/datasets/arashnic/dynamic-pricing-dataset

Message format:
{
    "ride_id":                  "RIDE-00000042",
    "event_time":               1709650000000,   <- Unix ms (Flink event time)
    "location_category":        "Urban",
    "vehicle_type":             "Premium",
    "time_of_booking":          "Night",
    "customer_loyalty_status":  "Silver",
    "number_of_riders":         78,
    "number_of_drivers":        41,
    "number_of_past_rides":     12,
    "average_ratings":          4.7,
    "expected_ride_duration":   36,
    "historical_cost_of_ride":  28.50
}

Usage:
    python src/data_generator.py
"""

import csv
import json
import time
import argparse
import os
from itertools import cycle
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = "ride-events"
CSV_PATH = os.getenv("DATA_FILE", "/app/data/dynamic_pricing.csv")

# Ride Event Factory
# One rideshare event per .csv row. 
def make_ride_event(row: dict,
                    ride_id: int,
                    delay_ms: int = 0) -> dict: # 0 so most events stay current + positive values indicate late/ out-of-order arrival.
                                                # to be used later for watermark handling

    now_ms        = int(time.time() * 1000) # current wall clock time
    event_time_ms = now_ms - delay_ms

    return {
        "ride_id":                 f"RIDE-{ride_id:08d}", # 0-padded synthetic ID for uniqueness in logs
        "location_category":       row["Location_Category"], # kept as string
        "vehicle_type":            row["Vehicle_Type"], # kept as string
        "time_of_booking":         row["Time_of_Booking"], # used later for pricing logic
        "customer_loyalty_status": row["Customer_Loyalty_Status"], # used later for pricing logic
        "number_of_riders":        int(row["Number_of_Riders"]), # converted to int
        "number_of_drivers":       int(row["Number_of_Drivers"]), # converted to int
        "number_of_past_rides":    int(row["Number_of_Past_Rides"]), # converted to int to identify past behavior
        "average_ratings":         float(row["Average_Ratings"]), # converted to float
        "expected_ride_duration":  int(row["Expected_Ride_Duration"]), # converted to int as it is in minutes
        "historical_cost_of_ride": float(row["Historical_Cost_of_Ride"]), # converted to float
        "event_time":              event_time_ms, # embedded event timestamp for event-time processing
    }

def load_csv_rows(csv_path: str) -> list[dict]:
    with open(csv_path, "r", encoding="utf-8", newline="") as f:  
        reader = csv.DictReader(f)
        rows = [row for row in reader if any(v not in (None, "") for v in row.values())]  # skip blank rows to avoid null events

    if not rows:
        raise ValueError(f"No usable rows found in {csv_path}")  # fail if file path/schema/content is wrong.

    return rows

def normal_ride_event(row: dict, ride_id: int) -> dict:
    return make_ride_event(row, ride_id)

# Kafka Producer
def connect_producer(bootstrap: str, retries: int = 15) -> KafkaProducer:  # 15 retries + 2 seconds gives 30 seconds to start
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,  # initial broker address used to discover cluster metadata
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # serialize python dicts to JSON bytes
                key_serializer=lambda k: k.encode("utf-8"),  # serialize string keys to bytes for Kafka
                batch_size=16_384,  # small batching
                linger_ms=10,       # increase throughput at cost of latency
            )
            print(f"[Generator] Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(f"[Generator] Kafka not ready, retrying "
                  f"({attempt}/{retries})...")
            time.sleep(2)  # small delay between retries

    raise RuntimeError(f"Could not connect to Kafka at {bootstrap} "
                       f"after {retries} attempts")

# Main Loop
def run(rate_per_second: float = 10.0):  # 10 events/sec to feel like live data streaming
    producer = connect_producer(KAFKA_BOOTSTRAP)
    rows     = load_csv_rows(CSV_PATH)

    row_source = cycle(rows)  # replay the dataset for a continuous demo stream

    total_sent     = 0
    ride_id        = 1
    sleep_interval = 1.0 / rate_per_second  # convert events/sec into pause between sends

    print(f"[Generator] Loaded {len(rows):,} rows from '{CSV_PATH}'")
    print(f"[Generator] Publishing to topic '{TOPIC}' at {rate_per_second} event/s")
    print("[Generator] Replaying CSV continuously")
    print("[Generator] Press Ctrl+C to stop\n")

    try:
        for row in row_source:
            event = normal_ride_event(row, ride_id)

            message_key = f"{event['location_category']}|{event['vehicle_type']}"  # segment-based Kafka key for easier grouping

            producer.send(
                TOPIC,
                key=message_key,
                value=event,
                timestamp_ms=event["event_time"],  # keep Kafka record timestamp aligned with the event timestamp
            )

            total_sent += 1

            if total_sent % 100 == 0:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                      f"Total published: {total_sent:,} rides")  # periodic progress update

            ride_id += 1
            time.sleep(sleep_interval)  # reduce send rate to match configured replay speed

    except KeyboardInterrupt:
        print(f"\n[Generator] Stopped. Total published: {total_sent:,}")

    finally:
        producer.flush()  # wait for buffered messages to finish sending before shutdown
        producer.close()

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka rideshare event generator")
    parser.add_argument("--rate", type=float, default=10.0)  # optional command line interface control for replay speed
    args = parser.parse_args()

    run(args.rate)