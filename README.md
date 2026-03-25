# Real-Time Ride-Hailing Demand Surge Detection

This project implements `INF2214 Assignment 2` as a PyFlink anomaly-detection pipeline for ride-hailing demand surges. It uses:

- `Option B - Anomaly Detection`
- `event time` semantics
- `15-second event-time windows`
- `5-second sliding updates` for a more active live demo
- keyed `ValueState` to maintain an exponential moving average (EMA) baseline
- `watermarks + allowed lateness` for late and out-of-order data

## Project Structure

```text
.
├── generators/
│   └── ride_request_generator.py
├── sample_output/
│   └── alerts.log
├── src/
│   ├── __init__.py
│   ├── schema.py
│   ├── stream_job.py
│   └── surge_detector.py
└── requirements.txt
```

## Environment Setup

### Prerequisites

- Python `3.10` or `3.11` recommended and safest for `apache-flink==1.19.3`
- Java `11+`
- Apache Flink installed locally, or a Python environment with `apache-flink`

### Install Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

The project pins `setuptools<81` because PyFlink `1.19.3` pulls in `apache-beam`, which still imports `pkg_resources`.

## Data Source

The project uses a synthetic ride-request generator that emits newline-delimited JSON events. Each event includes:

- `ride_id`
- `zone_id`
- `timestamp`
- `is_simulated_late`

The generator deliberately produces:

- deterministic traffic every `250 ms` for every zone
- recurring deterministic surge pulses across multiple zones
- the same replay every run by default
- optional deterministic delayed `Downtown` events with `--inject-delays`

## How To Run

Open three terminals from the project root.

### Terminal 1: start the generator

```bash
.venv/bin/python generators/ride_request_generator.py --mode socket-server --host 127.0.0.1 --port 9999
```

Optional delay-handling demo:

```bash
.venv/bin/python generators/ride_request_generator.py --mode socket-server --host 127.0.0.1 --port 9999 --max-seconds 50 --inject-delays
```

### Terminal 2: start the PyFlink job

```bash
.venv/bin/python src/stream_job.py --host 127.0.0.1 --port 9999
```

This job writes alert JSON lines to `surge_output.json`.

### Terminal 3: start the dashboard

```bash
.venv/bin/streamlit run dashboard.py
```

## Expected Pipeline

```text
Socket Source (ride requests)
-> JSON parsing
-> Timestamp assignment + watermarks
-> keyBy(zone_id)
-> Event-time timer windows (15 s / 5 s)
-> KeyedProcessFunction + ValueState EMA baseline
-> Alert sink
-> Drop records that arrive beyond allowed lateness
```

## Core Design Decisions

### Why Event Time

Ride requests can be delayed by mobile connectivity issues. Using event time prevents late-arriving records from creating false demand spikes based on arrival order instead of occurrence time.

### Why Time-Based Counting + Keyed State

The per-zone event-time timer logic captures short-term demand intensity, while keyed `ValueState` stores a longer-lived EMA baseline for that same zone. This is more stable than comparing the current window only to the immediately preceding one.

### Anomaly Definition

For each zone:

```text
surge if current_window_count > surge_threshold * historical_ema
```

Default parameters:

- `surge_threshold = 2.0`
- `alpha = 0.2`

## Late and Out-of-Order Data Strategy

- watermark delay: `3 seconds`
- allowed lateness: `10 seconds`
- records arriving after allowed lateness: ignored by the detector

This balances:

- `latency`: emit operational alerts quickly
- `completeness`: still count moderately late mobile events

## Sample Output

See:

- [sample_output/alerts.log](/Users/vanngo/Downloads/demand-surge/sample_output/alerts.log)

## Notes

- The assignment brief allows simulated generators, so this implementation uses a socket-fed synthetic stream for reproducibility.
- The default generator replay is deterministic even without extra flags.
- The Flink job writes alert records as JSONL to `surge_output.json`.
- If needed, the source can later be swapped for Kafka while keeping the same timestamping, windowing, and surge-detection logic.
