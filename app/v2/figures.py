"""Plotly figures of the phase 2 page (docs/plan/PLAN_api-v2.md, T9). Drawing only: the data
comes from api_client (DailyResult/RawResult series and the column selection), no request
is made here.

    rows(series)                       -> [Row]: the rows of the figure that have data
    data_figure(rows, mode, revision)  -> one make_subplots figure, shared x axes
    raw_trace_data(selected, lo, hi)   -> x, y and mode of one Full trace (thinned to the window)
    empty_figure(message)              -> a placeholder with a message

Rows, top to bottom, each left out without data: ground temperature (one line per depth,
taller), air temperature, relative humidity, barometric pressure (kPa), one row per
inclination angle (own y scale), compass heading, sensor temperature, battery voltage. Each
row has its own legend (Plotly's legend, legend2, ...) next to it.

Daily (``mode="daily"``): per series a mean line and a band from the daily minimum to the
daily maximum (two traces without legend entry before the line, the lower one filled to
the upper). Full (``mode="raw"``): one line per series, thinned by rawseries.thin_series to
about MAX_POINTS points, with ``meta`` = the API column key, so that a zoom can replace the
data trace by trace (callbacks.py). Lines break at gaps (daily: a missing day; raw: as
rawseries).
"""
from __future__ import annotations

import dataclasses
import datetime as dt

import numpy as np
import plotly.graph_objects as go
from plotly.colors import hex_to_rgb, sample_colorscale, unlabel_rgb
from plotly.subplots import make_subplots

from . import api_client, rawseries, texts

FONT = ("system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, "
        "'Noto Sans', 'Liberation Sans', sans-serif")
INK = "#212529"
MUTED = "#6c757d"
GRID = "#e9ecef"
AXIS = "#adb5bd"

# ground temperature by depth: Plasma from orange (shallow) to dark blue (deep), as TSP
DEPTH_SCALE = "Plasma"
DEPTH_SCALE_RANGE = (0.78, 0.02)
# other rows: a few distinct colours (Tableau 10)
PALETTE = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2",
           "#7f7f7f", "#bcbd22", "#17becf"]
BAND_ALPHA = 0.18

ROW_PX = 150            # plot height of a row
GROUND_PX = 320         # ... of the ground temperature row
GAP_PX = 46             # between rows (room for the row title)
MARGIN = dict(l=70, r=150, t=40, b=40)
EMPTY_HEIGHT = 420

MAX_POINTS = rawseries.MAX_POINTS        # per series and view (thin_series)
RAW_MARKERS_MAX = 300                    # raw lines with at most this many points get markers
DECIMALS = 4

UNITS = {"C": "°C", "degC": "°C", "deg": "°", "%": "%", "V": "V", "kPa": "kPa", "Pa": "Pa",
         "hPa": "hPa"}


@dataclasses.dataclass(frozen=True)
class Row:
    key: str            # "ground", "air", ..., "inclination:<column key>"
    title: str          # shown above the row
    unit: str | None    # unit of the values (after conversion), API spelling
    items: list         # [api_client.Selected], each with data
    tall: bool = False


def _has_data(sel) -> bool:
    return len(sel.series.time) > 0


def rows(series: dict) -> list:
    """The rows of the figure for a DailyResult/RawResult ``series`` dict, in page order;
    rows without data are left out."""
    out = []

    def add(key, title, items, tall=False):
        items = [s for s in items if _has_data(s)]
        if items:
            out.append(Row(key, title, items[0].unit, items, tall))

    add("ground", texts.ROW_GROUND, api_client.ground_temperature(series), tall=True)
    add("air", texts.ROW_AIR, api_client.weather(series, "AirTemp"))
    add("humidity", texts.ROW_HUMIDITY, api_client.weather(series, "RelHum"))
    add("pressure", texts.ROW_PRESSURE, api_client.weather(series, "BarometricPressure"))
    for sel in api_client.inclination(series):
        add(f"inclination:{sel.key}", texts.ROW_INCLINATION.format(name=sel.label), [sel])
    add("heading", texts.ROW_HEADING, api_client.heading(series))
    add("sensor_temperature", texts.ROW_SENSOR_TEMPERATURE, api_client.sensor_temperature(series))
    add("battery", texts.ROW_BATTERY, api_client.battery(series))
    return out


def unit_text(unit) -> str:
    return UNITS.get(unit, unit or "")


def _base_layout(height: int) -> dict:
    return dict(template="none", height=height, font=dict(family=FONT, size=13, color=INK),
                paper_bgcolor="white", plot_bgcolor="white",
                hoverlabel=dict(font=dict(family=FONT)))


def empty_figure(message: str, height: int = EMPTY_HEIGHT) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **_base_layout(height), xaxis=dict(visible=False), yaxis=dict(visible=False),
        margin=dict(l=20, r=20, t=20, b=20),
        annotations=[dict(text=message, x=0.5, y=0.5, xref="paper", yref="paper",
                          showarrow=False, font=dict(size=15, color=MUTED))])
    return fig


# ---- colours ----------------------------------------------------------------------------------

def depth_colors(n: int) -> list:
    """n colours (rgb(...) strings), shallow (warm) to deep (cool), evenly spaced by rank."""
    if n <= 0:
        return []
    hi, lo = DEPTH_SCALE_RANGE
    points = [hi] if n == 1 else [hi + (lo - hi) * i / (n - 1) for i in range(n)]
    return sample_colorscale(DEPTH_SCALE, points)


def _rgba(color: str, alpha: float) -> str:
    r, g, b = hex_to_rgb(color) if color.startswith("#") else unlabel_rgb(color)
    return f"rgba({int(round(r))}, {int(round(g))}, {int(round(b))}, {alpha})"


def row_colors(row: Row) -> list:
    if row.key == "ground":
        return depth_colors(len(row.items))
    return [PALETTE[i % len(PALETTE)] for i in range(len(row.items))]


# ---- trace data -------------------------------------------------------------------------------

def _values(v) -> list:
    """Floats rounded to DECIMALS, None for NaN (a line break)."""
    out = np.round(np.asarray(v, dtype=float), DECIMALS).tolist()
    return [None if x != x else x for x in out]


def daily_trace_data(s) -> dict:
    """x (days), mean, min, max of a DailySeries, with a break (None) at every missing day."""
    t = s.time.astype("datetime64[D]")
    arrays = [s.mean, s.min, s.max]
    if len(t) > 1:
        idx = np.where(np.diff(t) > np.timedelta64(1, "D"))[0] + 1
        if len(idx):
            t = np.insert(t, idx, t[idx - 1] + np.timedelta64(1, "D"))
            arrays = [np.insert(np.asarray(a, dtype=float), idx, np.nan) for a in arrays]
    return {"x": np.datetime_as_string(t, unit="D").tolist(),
            "mean": _values(arrays[0]), "min": _values(arrays[1]), "max": _values(arrays[2])}


def raw_mode(n_points: int) -> str:
    return "lines+markers" if n_points <= RAW_MARKERS_MAX else "lines"


def raw_trace_data(sel, lo, hi, max_points: int | None = None) -> dict:
    """x, y and mode of one Full trace: the raw series of ``sel`` in [lo, hi], thinned to
    about ``max_points`` points (MAX_POINTS), plus ``bin_seconds`` (None: not thinned)."""
    t, v, _thinned, bin_s = rawseries.thin_series(sel.series.time, sel.series.value, lo, hi,
                                                  max_points or MAX_POINTS)
    y = _values(v)
    return {"x": np.datetime_as_string(t, unit="s").tolist(), "y": y,
            "mode": raw_mode(sum(1 for x in y if x is not None)), "bin_seconds": bin_s}


# ---- the figure -------------------------------------------------------------------------------

def _axis_suffix(r: int) -> str:
    return "" if r == 1 else str(r)


def data_figure(rows_: list, mode: str, revision: str, window=None, max_points=None):
    """(figure, trace keys, bin seconds) of the rows. ``mode`` "daily" (DailySeries) or
    "raw" (RawSeries, thinned to ``window`` (lo, hi) datetimes). Trace keys: the API column key
    of every trace in figure order (None for the band traces), for the zoom's Patch.
    ``revision`` is the figure's uirevision (a new value resets the user's zoom). Bin
    seconds: the largest bin width of a thinned series (None: nothing thinned)."""
    if not rows_:
        return empty_figure(texts.FIGURE_EMPTY), [], None
    n = len(rows_)
    px = [GROUND_PX if r.tall else ROW_PX for r in rows_]
    plot_px = sum(px) + GAP_PX * (n - 1)
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True, row_heights=px,
                        vertical_spacing=GAP_PX / plot_px if n > 1 else 0,
                        subplot_titles=[r.title for r in rows_])
    keys, bins = [], []
    raw = mode == "raw"
    for r, row in enumerate(rows_, start=1):
        legend = f"legend{_axis_suffix(r)}"
        unit = unit_text(row.unit)
        many = len(row.items) > 1
        for sel, color in zip(row.items, row_colors(row)):
            group = f"{row.key}|{sel.key}"
            common = dict(name=sel.label, legend=legend, legendgroup=group, showlegend=many)
            hover_unit = unit_text(sel.unit)
            if raw:
                d = raw_trace_data(sel, *window, max_points=max_points)
                if d["bin_seconds"] is not None:
                    bins.append(d["bin_seconds"])
                fig.add_trace(go.Scatter(
                    x=d["x"], y=d["y"], mode=d["mode"], meta=sel.key, connectgaps=False,
                    line=dict(color=color, width=1.4), marker=dict(color=color, size=4),
                    hovertemplate=texts.HOVER_RAW.format(name=sel.label, unit=hover_unit),
                    **common), row=r, col=1)
                keys.append(sel.key)
                continue
            d = daily_trace_data(sel.series)
            band = dict(x=d["x"], mode="lines", line=dict(width=0, color=color), hoverinfo="skip",
                        connectgaps=False, legend=legend, legendgroup=group, showlegend=False)
            fig.add_trace(go.Scatter(y=d["max"], **band), row=r, col=1)
            fig.add_trace(go.Scatter(y=d["min"], fill="tonexty", fillcolor=_rgba(color, BAND_ALPHA),
                                     **band), row=r, col=1)
            fig.add_trace(go.Scatter(
                x=d["x"], y=d["mean"], customdata=list(zip(d["min"], d["max"])), mode="lines",
                connectgaps=False, line=dict(color=color, width=1.5),
                hovertemplate=texts.HOVER_DAILY.format(name=sel.label, unit=hover_unit),
                **common), row=r, col=1)
            keys.extend([None, None, sel.key])
        fig.update_yaxes(title=dict(text=unit, standoff=6), row=r, col=1)
    height = plot_px + MARGIN["t"] + MARGIN["b"]
    fig.update_layout(**_base_layout(height), margin=MARGIN, hovermode="x unified",
                      uirevision=revision)
    axis = dict(showgrid=True, gridcolor=GRID, showline=True, linecolor=AXIS, ticks="outside",
                tickcolor=AXIS, zeroline=False)
    fig.update_xaxes(type="date", **axis)
    fig.update_yaxes(**axis)
    if window is not None:
        fig.update_xaxes(range=[_plotly_time(t) for t in window])
    for r in range(1, n + 1):
        top = fig.layout[f"yaxis{_axis_suffix(r)}"].domain[1]
        fig.update_layout({f"legend{_axis_suffix(r)}": dict(
            x=1.01, xanchor="left", y=top, yanchor="top", font=dict(size=12),
            bgcolor="rgba(255,255,255,0)", itemclick="toggle", itemdoubleclick="toggleothers")})
    for a in fig.layout.annotations:        # the row titles, left-aligned above each row
        a.update(x=0, xanchor="left", font=dict(size=13, color=MUTED))
    return fig, keys, (max(bins) if bins else None)


def _plotly_time(t) -> str:
    """A datetime as Plotly's axis string (UTC, no zone)."""
    if getattr(t, "tzinfo", None) is not None:
        t = t.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return t.strftime("%Y-%m-%d %H:%M:%S")
