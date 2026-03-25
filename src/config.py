from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "surge_output.json"
TIME_SEMANTICS = "Event Time"
WINDOW_SECONDS = 15
WINDOW_SLIDE_SECONDS = 5
WATERMARK_DELAY_SECONDS = 3
ALLOWED_LATENESS_SECONDS = 10
SURGE_THRESHOLD = 2.0
EMA_ALPHA = 0.2
