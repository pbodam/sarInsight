"""Plotly helpers for SAR charts."""

import re
from typing import Any, Optional

import pandas as pd

# Kept for compatibility with app imports; no forced initial window is applied.
DEFAULT_INITIAL_WINDOW_SEC = 24 * 60 * 60

_TIME_RE = re.compile(r"(\d{1,2}:\d{2}:\d{2}(?:\s?[AP]M)?)", re.IGNORECASE)


def _normalize_time_value(value):
    """Keep only SAR time text (no synthetic date/year)."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%H:%M:%S")
    s = str(value).strip()
    if not s:
        return None
    m = _TIME_RE.search(s)
    if m:
        return m.group(1).upper().replace("  ", " ")
    return s


def coerce_time_column(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Normalize ``time`` to plain text labels from SAR output."""
    if df is None or df.empty or "time" not in df.columns:
        return df
    out = df.copy()
    out["time"] = out["time"].map(_normalize_time_value)
    out = out.dropna(subset=["time"])
    return out


def pick_anchor_time_series(
    cpu: Optional[pd.DataFrame],
    disk: Optional[pd.DataFrame],
    net: Optional[pd.DataFrame],
    extra: Optional[dict] = None,
) -> Optional[pd.Series]:
    """Return first available timeline (not used for forced range anymore)."""
    for df in (cpu, disk, net):
        if df is not None and not df.empty and "time" in df.columns:
            return df["time"]
    if extra:
        for v in extra.values():
            if v is not None and not getattr(v, "empty", True) and "time" in v.columns:
                return v["time"]
    return None


def initial_x_range_from_series(
    time_series: Optional[pd.Series], window_sec: int = DEFAULT_INITIAL_WINDOW_SEC
) -> Optional[list]:
    """Do not force an initial range; show full available time by default."""
    return None


def finalize_sar_figure_html(fig: Any, x_range: Optional[list]) -> Optional[str]:
    """Return Plotly HTML without forcing x-axis ticks or range."""
    if fig is None:
        return None
    return fig.to_html(full_html=False)
