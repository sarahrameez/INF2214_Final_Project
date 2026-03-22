# Real-Time Dynamic Pricing for Rideshare App with Apache Kafka and PyFlink

### Choices
1. Use Case: Option B (Anomaly Detection) used because we will be identifying pricing anomalies against recent rideshare history, using sliding event-time windows.
2. Time Semantics: Event time used as the event timestamp will be embedded in the record and correctness matters in our use case.
   1. Late data handling: we will be employing watermarks + allowed lateness
3. State Pattern: This project uses "windowed aggregation", using a sliding event-time window with a length of 5 minutes and a slide of 1 minute.
4. Key: A composite key of "location_category | vehicle_type" will be used so each market segment gets its own windowed metrics. For each windowed segment, the following metrics will be computed:
   1. Average historical ride cost
   2. Average demand ratio = riders/drivers
   3. ride count
   4. maximum ride cost
5. Flagging Rules: A window for a segment will be considered anomalous when one or more of the following conditions are met:
   1. Average demand ratio > threshold,
   2. Average historic ride cost is unsually high,
   3. Current segment cost exceeds the recent window average by 10%.

