from __future__ import annotations

from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.common.typeinfo import Types

try:
    from .schema import format_alert
except ImportError:
    from schema import format_alert


MIN_WATERMARK = -(1 << 63)


class SurgeDetector(KeyedProcessFunction):
    """Maintains per-zone state and emits event-time surge alerts."""

    def __init__(
        self,
        alpha: float = 0.2,
        surge_threshold: float = 2.0,
        window_seconds: int = 15,
        slide_seconds: int = 15,
        allowed_lateness_seconds: int = 120,
    ):
        self.alpha = alpha
        self.surge_threshold = surge_threshold
        self.window_ms = window_seconds * 1000
        self.slide_ms = slide_seconds * 1000
        self.allowed_lateness_ms = allowed_lateness_seconds * 1000
        self.timestamps_state = None
        self.ema_state = None
        self.first_event_time_state = None

    def open(self, runtime_context):
        self.timestamps_state = runtime_context.get_list_state(
            ListStateDescriptor("event_timestamps", Types.LONG())
        )
        self.ema_state = runtime_context.get_state(
            ValueStateDescriptor("historical_ema", Types.DOUBLE())
        )
        self.first_event_time_state = runtime_context.get_state(
            ValueStateDescriptor("first_event_time", Types.LONG())
        )

    def process_element(self, value, ctx):
        event_time = int(value[3])
        first_event_time = self.first_event_time_state.value()
        if first_event_time is None or event_time < first_event_time:
            self.first_event_time_state.update(event_time)

        latest_relevant_window_end = self._latest_relevant_window_end(event_time)
        current_watermark = ctx.timer_service().current_watermark()

        if (
            current_watermark != MIN_WATERMARK
            and current_watermark > latest_relevant_window_end + self.allowed_lateness_ms
        ):
            return

        self.timestamps_state.add(event_time)

        timer_timestamp = self._align_to_next_slide(event_time)
        while timer_timestamp <= latest_relevant_window_end:
            ctx.timer_service().register_event_time_timer(timer_timestamp)
            timer_timestamp += self.slide_ms

    def on_timer(self, timestamp, ctx):
        event_timestamps = list(self.timestamps_state.get())
        window_start = timestamp - self.window_ms
        current_count = sum(
            1 for event_time in event_timestamps if window_start <= event_time < timestamp
        )
        previous_average = self.ema_state.value()
        first_event_time = self.first_event_time_state.value()
        warming_up = (
            first_event_time is None or timestamp <= first_event_time + self.window_ms
        )

        if previous_average is None or warming_up:
            baseline = float(current_count)
            status = "INITIALIZING"
            self.ema_state.update(baseline)
        else:
            baseline = float(previous_average)
            is_surge = current_count > (self.surge_threshold * previous_average)
            status = "SURGE_ALERT" if is_surge else "NORMAL"
            new_average = (self.alpha * current_count) + (
                (1 - self.alpha) * previous_average
            )
            self.ema_state.update(float(new_average))

        cutoff = timestamp - self.window_ms - self.allowed_lateness_ms
        retained_timestamps = [
            event_time for event_time in event_timestamps if event_time >= cutoff
        ]
        self.timestamps_state.update(retained_timestamps)

        yield format_alert(
            zone_id=ctx.get_current_key(),
            window_end_millis=timestamp,
            current_count=current_count,
            baseline=baseline,
            status=status,
        )

    def _align_to_next_slide(self, event_time: int) -> int:
        remainder = event_time % self.slide_ms
        if remainder == 0:
            return event_time + self.slide_ms
        return event_time + (self.slide_ms - remainder)

    def _latest_relevant_window_end(self, event_time: int) -> int:
        latest_window_end = self._align_to_next_slide(event_time)
        next_window_end = latest_window_end + self.slide_ms

        while next_window_end < event_time + self.window_ms:
            latest_window_end = next_window_end
            next_window_end += self.slide_ms

        return latest_window_end
