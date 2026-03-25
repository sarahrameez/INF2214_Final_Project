from __future__ import annotations

import html
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from src.config import (
    ALLOWED_LATENESS_SECONDS,
    OUTPUT_FILE,
    SURGE_THRESHOLD,
    TIME_SEMANTICS,
    WATERMARK_DELAY_SECONDS,
    WINDOW_SECONDS,
    WINDOW_SLIDE_SECONDS,
)

REFRESH_INTERVAL = "1s"
DATA_FILE = OUTPUT_FILE
STATUS_RANK = {"SURGE_ALERT": 0, "INITIALIZING": 1, "NORMAL": 2}
CHART_TEXT_COLOR = "#d8e4e0"
CHART_MUTED_TEXT_COLOR = "#9fb0ab"
CHART_GRID_COLOR = "#22313a"
REQUEST_COLOR = "#67c7ff"
BASELINE_COLOR = "#f3c969"
SURGE_COLOR = "#ff8c69"


def parse_json_line(raw_line: str) -> dict | None:
    stripped = raw_line.strip()
    if not stripped:
        return None

    payload = json.loads(stripped)
    required = {"timestamp", "zone", "request_count", "ema_baseline", "status"}
    missing = required.difference(payload)
    if missing:
        missing_fields = ", ".join(sorted(missing))
        raise ValueError(f"Missing required fields: {missing_fields}")

    return {
        "timestamp": payload["timestamp"],
        "zone": str(payload["zone"]),
        "request_count": int(payload["request_count"]),
        "ema_baseline": float(payload["ema_baseline"]),
        "status": str(payload["status"]),
    }


def load_records(file_path: Path) -> tuple[pd.DataFrame, str]:
    if not file_path.exists():
        return pd.DataFrame(), f"Waiting for data file: {file_path.name}"

    rows: list[dict] = []
    malformed_lines = 0
    last_error = ""

    with file_path.open("r", encoding="utf-8") as source:
        for raw_line in source:
            try:
                parsed = parse_json_line(raw_line)
            except Exception as exc:
                malformed_lines += 1
                last_error = str(exc)
                continue

            if parsed is not None:
                rows.append(parsed)

    if not rows:
        return pd.DataFrame(), f"{file_path.name} has no valid rows yet."

    df = pd.DataFrame.from_records(rows)
    df["row_id"] = range(len(df))
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values(
        ["timestamp", "row_id"], kind="stable"
    ).reset_index(drop=True)

    status_parts = ["Live stream connected."]
    if malformed_lines:
        status_parts.append(f"Skipped {malformed_lines} malformed line(s).")
    if last_error:
        status_parts.append(f"Last parse issue: {last_error}")
    return df, " ".join(status_parts)


def format_zone_label(zone: str) -> str:
    return zone.replace("_", " ")


def prepare_display_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    display_df = (
        df.groupby(["timestamp", "zone"], as_index=False, sort=False)
        .tail(1)
        .sort_values(["timestamp", "row_id"], kind="stable")
        .reset_index(drop=True)
    )
    display_df["window_start"] = display_df["timestamp"] - pd.to_timedelta(WINDOW_SECONDS, unit="s")
    display_df["pressure_ratio"] = display_df["request_count"] / display_df[
        "ema_baseline"
    ].where(display_df["ema_baseline"] > 0, pd.NA)
    display_df["pressure_delta"] = (
        display_df["request_count"] - display_df["ema_baseline"]
    )
    display_df["zone_label"] = display_df["zone"].map(format_zone_label)
    display_df["status_rank"] = (
        display_df["status"].map(STATUS_RANK).fillna(len(STATUS_RANK)).astype(int)
    )
    return display_df


def latest_by_zone(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    return (
        df.sort_values(["timestamp", "row_id"], kind="stable")
        .groupby("zone", as_index=False, sort=False)
        .tail(1)
        .sort_values(["status_rank", "request_count", "zone"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


def build_zone_status_table(latest_df: pd.DataFrame) -> pd.DataFrame:
    if latest_df.empty:
        return latest_df.copy()

    status_df = latest_df[
        ["zone", "request_count", "ema_baseline", "pressure_ratio", "status"]
    ].copy()
    status_df["zone"] = status_df["zone"].map(format_zone_label)
    status_df["pressure_ratio"] = status_df["pressure_ratio"].round(2)
    status_df["ema_baseline"] = status_df["ema_baseline"].round(1)
    status_df = status_df.rename(
        columns={
            "zone": "Zone",
            "request_count": "Requests",
            "ema_baseline": "Baseline",
            "pressure_ratio": "Pressure",
            "status": "Status",
        }
    )
    return status_df


def build_recent_table(df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    recent_df = (
        df.sort_values(["timestamp", "row_id"], kind="stable")
        .tail(limit)
        .copy()
        .sort_values(["timestamp", "status_rank", "zone"], ascending=[False, True, True])
    )
    recent_df["window_start"] = recent_df["window_start"].dt.strftime("%Y-%m-%d %H:%M:%S")
    recent_df["timestamp"] = recent_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    recent_df["zone"] = recent_df["zone"].map(format_zone_label)
    recent_df["ema_baseline"] = recent_df["ema_baseline"].round(1)
    recent_df["pressure_ratio"] = recent_df["pressure_ratio"].round(2)
    recent_df = recent_df.rename(
        columns={
            "window_start": "Window Start",
            "timestamp": "Window End",
            "zone": "Zone",
            "request_count": "Requests",
            "ema_baseline": "Baseline",
            "pressure_ratio": "Pressure",
            "status": "Status",
        }
    )
    return recent_df[
        ["Window Start", "Window End", "Zone", "Requests", "Baseline", "Pressure", "Status"]
    ]


def build_trend_chart(df: pd.DataFrame):
    if df.empty:
        return None

    chart_df = df[
        ["timestamp", "zone_label", "request_count", "ema_baseline", "status", "row_id"]
    ].copy()
    zone_order = (
        chart_df.sort_values("row_id", kind="stable")["zone_label"].drop_duplicates().tolist()
    )

    base = alt.Chart(chart_df).encode(
        x=alt.X(
            "timestamp:T",
            title="Window End",
            axis=alt.Axis(format="%H:%M:%S", labelAngle=0),
        ),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Window End"),
            alt.Tooltip("zone_label:N", title="Zone"),
            alt.Tooltip("request_count:Q", title="Requests"),
            alt.Tooltip("ema_baseline:Q", title="Baseline", format=".1f"),
            alt.Tooltip("status:N", title="Status"),
        ],
    )

    request_area = base.mark_area(color=REQUEST_COLOR, opacity=0.16).encode(
        y=alt.Y("request_count:Q", title="Requests")
    )
    request_line = base.mark_line(color=REQUEST_COLOR, strokeWidth=2.4).encode(
        y=alt.Y("request_count:Q", title="Requests")
    )
    baseline_line = base.mark_line(
        color=BASELINE_COLOR, strokeDash=[5, 4], strokeWidth=1.8
    ).encode(y=alt.Y("ema_baseline:Q", title="Requests"))
    surge_points = (
        base.transform_filter(alt.datum.status == "SURGE_ALERT")
        .mark_point(
            color=SURGE_COLOR,
            filled=True,
            size=90,
            opacity=1,
            stroke="#fff3ec",
            strokeWidth=1.2,
        )
        .encode(y=alt.Y("request_count:Q", title="Requests"))
    )

    chart = (
        alt.layer(request_area, baseline_line, request_line, surge_points)
        .properties(height=76)
        .facet(
            row=alt.Row(
                "zone_label:N",
                title=None,
                sort=zone_order,
                header=alt.Header(
                    labelAngle=0,
                    labelAlign="left",
                    labelAnchor="start",
                    labelOrient="left",
                    labelPadding=10,
                ),
            ),
            spacing=10,
        )
        .resolve_scale(y="independent")
    )
    return style_chart(chart)


def build_baseline_comparison_chart(latest_df: pd.DataFrame):
    if latest_df.empty:
        return None

    chart_df = latest_df[["zone", "request_count", "ema_baseline"]].rename(
        columns={"request_count": "Current Requests", "ema_baseline": "EMA Baseline"}
    )
    chart_df["zone"] = chart_df["zone"].map(format_zone_label)
    chart_df = chart_df.melt(
        id_vars="zone", var_name="Series", value_name="Value"
    )
    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("zone:N", title="Zone"),
            xOffset="Series:N",
            y=alt.Y("Value:Q", title="Requests"),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(range=["#ff8c69", "#5e7cff"]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("zone:N", title="Zone"),
                alt.Tooltip("Series:N", title="Series"),
                alt.Tooltip("Value:Q", title="Value", format=".1f"),
            ],
        )
        .properties(height=280)
    )
    return style_chart(chart)


def build_surge_count_chart(df: pd.DataFrame):
    if df.empty:
        return None

    surge_df = (
        df.loc[df["status"] == "SURGE_ALERT"]
        .groupby("zone", as_index=False)
        .size()
        .rename(columns={"size": "surge_alerts"})
    )
    if surge_df.empty:
        return None

    surge_df["zone"] = surge_df["zone"].map(format_zone_label)

    chart = (
        alt.Chart(surge_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color="#ff6b57")
        .encode(
            x=alt.X("zone:N", title="Zone"),
            y=alt.Y("surge_alerts:Q", title="Alert Count"),
            tooltip=[
                alt.Tooltip("zone:N", title="Zone"),
                alt.Tooltip("surge_alerts:Q", title="Surge Alerts"),
            ],
        )
        .properties(height=280)
    )
    return style_chart(chart)


def style_chart(chart):
    return (
        chart.properties(background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(
            domainColor=CHART_GRID_COLOR,
            gridColor=CHART_GRID_COLOR,
            labelColor=CHART_MUTED_TEXT_COLOR,
            tickColor=CHART_GRID_COLOR,
            titleColor=CHART_TEXT_COLOR,
        )
        .configure_header(
            labelColor=CHART_TEXT_COLOR,
            titleColor=CHART_TEXT_COLOR,
        )
        .configure_legend(
            labelColor=CHART_MUTED_TEXT_COLOR,
            titleColor=CHART_TEXT_COLOR,
        )
    )


def style_status_table(df: pd.DataFrame):
    def row_style(row: pd.Series) -> list[str]:
        status = row.get("Status", "")
        if status == "SURGE_ALERT":
            return ["background-color: rgba(166, 44, 32, 0.18); color: #fff1ed;"] * len(row)
        if status == "INITIALIZING":
            return ["background-color: rgba(162, 126, 40, 0.16); color: #fff4d6;"] * len(row)
        if status == "NORMAL":
            return ["background-color: rgba(20, 94, 73, 0.16); color: #e8fff8;"] * len(row)
        return [""] * len(row)

    return df.style.apply(row_style, axis=1)


def render_header() -> None:
    st.markdown(
        f"""
        <div class="header-shell">
            <div>
                <p class="eyebrow">Stateful Streaming Monitor</p>
                <h1>Ride-Hailing Demand Dashboard</h1>
                <p class="lede">
                    Zone-level demand snapshots for the PyFlink surge detector. The page is optimized
                    for current status, pressure against baseline, and recent surge history.
                </p>
            </div>
            <div class="config-shell">
                <div class="config-pill">{TIME_SEMANTICS}</div>
                <div class="config-pill">{WINDOW_SECONDS}s window / {WINDOW_SLIDE_SECONDS}s slide</div>
                <div class="config-pill">{SURGE_THRESHOLD}x EMA threshold</div>
                <div class="config-pill">{WATERMARK_DELAY_SECONDS}s watermark</div>
                <div class="config-pill">{ALLOWED_LATENESS_SECONDS}s lateness</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_source_band(message: str, raw_df: pd.DataFrame, display_df: pd.DataFrame) -> None:
    latest_ts = (
        display_df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S UTC")
        if not display_df.empty
        else "n/a"
    )
    safe_message = html.escape(message)
    status_label = "LIVE" if not raw_df.empty else "WAITING"
    status_class = "live" if not raw_df.empty else "waiting"
    st.markdown(
        f"""
        <div class="source-band">
            <div class="source-copy">
                <span class="status-chip {status_class}">{status_label}</span>
                <span>{safe_message}</span>
            </div>
            <div class="source-meta">
                <span>Source: {DATA_FILE.name}</span>
                <span>Rows loaded: {len(raw_df.index)}</span>
                <span>Snapshots shown: {len(display_df.index)}</span>
                <span>Latest window end: {latest_ts}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_summary_metrics(display_df: pd.DataFrame, latest_df: pd.DataFrame) -> None:
    total_snapshots = int(len(display_df.index)) if not display_df.empty else 0
    surge_alerts = (
        int((display_df["status"] == "SURGE_ALERT").sum()) if not display_df.empty else 0
    )
    active_surges = (
        int((latest_df["status"] == "SURGE_ALERT").sum()) if not latest_df.empty else 0
    )
    peak_requests = int(display_df["request_count"].max()) if not display_df.empty else 0

    cols = st.columns(4)
    cols[0].metric("Window Snapshots", total_snapshots)
    cols[1].metric("Active Zone Surges", active_surges)
    cols[2].metric("Surge Alerts", surge_alerts)
    cols[3].metric("Peak Request Count", peak_requests)


def render_empty_state(message: str) -> None:
    st.info(
        f"{message} Start the generator and Flink job, then wait for the first event-time "
        "window to close."
    )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(194, 72, 33, 0.12), transparent 28%),
                radial-gradient(circle at top right, rgba(62, 130, 105, 0.10), transparent 24%),
                linear-gradient(180deg, #091115 0%, #0d151a 48%, #10171d 100%);
            color: #eff5f3;
        }
        html, body, [class*="css"] {
            font-family: "Avenir Next", "Helvetica Neue", sans-serif;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2.5rem;
        }
        .header-shell {
            display: grid;
            grid-template-columns: minmax(0, 2.2fr) minmax(280px, 1fr);
            gap: 1rem;
            padding: 1.2rem 1.3rem 1.25rem;
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 22px;
            background: linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.025));
            margin-bottom: 1rem;
        }
        .eyebrow {
            margin: 0 0 0.4rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #f2a589;
            font-size: 0.78rem;
            font-weight: 700;
        }
        .header-shell h1 {
            margin: 0;
            font-size: clamp(2.1rem, 3vw, 3.3rem);
            line-height: 0.98;
            letter-spacing: -0.03em;
            color: #f7fbfa;
        }
        .lede {
            margin: 0.85rem 0 0;
            max-width: 44rem;
            color: #c7d5d1;
            font-size: 1rem;
            line-height: 1.55;
        }
        .config-shell {
            display: flex;
            flex-wrap: wrap;
            align-content: start;
            justify-content: flex-start;
            gap: 0.55rem;
        }
        .config-pill {
            padding: 0.55rem 0.8rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.055);
            border: 1px solid rgba(255, 255, 255, 0.08);
            color: #e8f0ed;
            font-size: 0.9rem;
            white-space: nowrap;
        }
        .source-band {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            gap: 0.8rem 1rem;
            padding: 0.9rem 1rem;
            margin: 0.5rem 0 1rem;
            border-radius: 18px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            background: rgba(255, 255, 255, 0.04);
        }
        .source-copy {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.7rem;
            color: #edf7f4;
        }
        .source-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            color: #b7c9c3;
            font-size: 0.92rem;
        }
        .status-chip {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.06em;
        }
        .status-chip.live {
            color: #d8fff2;
            background: rgba(17, 112, 78, 0.28);
        }
        .status-chip.waiting {
            color: #fff1d1;
            background: rgba(144, 108, 26, 0.28);
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.045);
            border: 1px solid rgba(255, 255, 255, 0.07);
            border-radius: 18px;
            padding: 0.95rem 1rem;
        }
        div[data-testid="stMetricLabel"] {
            color: #b9cac6;
        }
        div[data-testid="stMetricValue"] {
            color: #f8fbfb;
        }
        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
        }
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(255, 255, 255, 0.03);
        }
        @media (max-width: 900px) {
            .header-shell {
                grid-template-columns: 1fr;
            }
            .source-meta {
                gap: 0.55rem 1rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.fragment(run_every=REFRESH_INTERVAL)
def render_live_dashboard() -> None:
    raw_df, status_message = load_records(DATA_FILE)
    display_df = prepare_display_frame(raw_df)
    latest_df = latest_by_zone(display_df)

    render_source_band(status_message, raw_df, display_df)
    render_summary_metrics(display_df, latest_df)

    if display_df.empty:
        render_empty_state(status_message)
        return

    trend_col, status_col = st.columns((2.2, 1), gap="large")
    with trend_col:
        with st.container(border=True):
            st.subheader("Windowed Request Counts by Zone")
            st.caption(
                "Each zone keeps its own live lane so equal-count series remain visible."
            )
            st.altair_chart(build_trend_chart(display_df), use_container_width=True)

    with status_col:
        with st.container(border=True):
            st.subheader("Latest Zone Status")
            st.caption("Current request pressure against each zone's EMA baseline.")
            st.dataframe(
                style_status_table(build_zone_status_table(latest_df)),
                width="stretch",
                hide_index=True,
            )

    charts_left, charts_right = st.columns(2, gap="large")
    with charts_left:
        with st.container(border=True):
            st.subheader("Current Requests vs Baseline")
            st.caption("Grouped bars compare the latest count to the live EMA baseline.")
            st.altair_chart(
                build_baseline_comparison_chart(latest_df), use_container_width=True
            )

    with charts_right:
        with st.container(border=True):
            st.subheader("Surge Alert Count by Zone")
            st.caption("Historical surge frequency across the current output file.")
            surge_chart = build_surge_count_chart(display_df)
            if surge_chart is None:
                st.info("No surge alerts have been recorded yet.")
            else:
                st.altair_chart(surge_chart, use_container_width=True)

    with st.container(border=True):
        st.subheader("Recent Window Evaluations")
        st.caption("Most recent deduplicated snapshots, sorted with surge states first.")
        st.dataframe(
            style_status_table(build_recent_table(display_df)),
            width="stretch",
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title="Ride-Hailing Demand Dashboard", layout="wide")
    alt.data_transformers.disable_max_rows()
    apply_theme()
    render_header()
    render_live_dashboard()


if __name__ == "__main__":
    main()
