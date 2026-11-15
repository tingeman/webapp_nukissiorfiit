"""Plotly figures of the phase 2 page (docs/plan/PLAN_api-v2.md, T9). Drawing only: the data
comes from api_client (DailyResult/RawResult series and the column selection), no request
is made here.

    rows(series)                       -> [Row]: the rows of the figure that have data
    data_figure(rows, mode, revision)  -> one make_subplots figure, shared x axes
    raw_trace_data(selected, lo, hi)   -> x, y and mode of one Full trace (thinned to the window)
    empty_figure(message)              -> a placeholder with a message
    profile_figure(day, annual, ...)   -> the ground temperature profile of one day (T10)
    day_marker(day)                    -> a shape marking that day on the data figure
    change_shapes(changes)             -> dashed lines at the logger changes (T11)

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

All loggers (T11): the series come from join.join (one series per depth or series name and
station, spanning the loggers). A joined series' line breaks at every logger change (its
``breaks``), and a raw series is thinned per logger segment over the same window (same bin
width). Inclination rows are then one per series name. With several stations (Mast 9) each
row's traces are grouped by station (legend groups titled with the station identifier, the
ground temperature colours per depth within a station, the second station's lines dotted).
The logger changes are dashed vertical lines (``change_shapes``, layout shapes) with the
logger's name above the figure (annotations).
"""
from __future__ import annotations

import dataclasses
import datetime as dt

import numpy as np
import plotly.graph_objects as go
from plotly.colors import hex_to_rgb, sample_colorscale, unlabel_rgb
from plotly.subplots import make_subplots

from . import api_client, join, rawseries, texts

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

    stations = []       # joined series: the stations in order of appearance
    for s in series.values():
        st = join.station(s)
        if st is not None and st not in stations:
            stations.append(st)

    def by_station(items):      # several stations: a station's items together, stable
        if len(stations) < 2:
            return items
        return sorted(items, key=lambda sel: stations.index(join.station(sel)))

    add("ground", texts.ROW_GROUND, by_station(api_client.ground_temperature(series)), tall=True)
    add("air", texts.ROW_AIR, by_station(api_client.weather(series, "AirTemp")))
    add("humidity", texts.ROW_HUMIDITY, by_station(api_client.weather(series, "RelHum")))
    add("pressure", texts.ROW_PRESSURE, by_station(api_client.weather(series, "BarometricPressure")))
    angles = api_client.inclination(series)
    if stations:        # joined: one row per series name (each station's series in it)
        for name in dict.fromkeys(sel.label for sel in angles):
            add(f"inclination:{name}", texts.ROW_INCLINATION.format(name=name),
                by_station([sel for sel in angles if sel.label == name]))
    else:
        for sel in angles:
            add(f"inclination:{sel.key}", texts.ROW_INCLINATION.format(name=sel.label), [sel])
    add("heading", texts.ROW_HEADING, by_station(api_client.heading(series)))
    add("sensor_temperature", texts.ROW_SENSOR_TEMPERATURE, by_station(api_client.sensor_temperature(series)))
    add("battery", texts.ROW_BATTERY, by_station(api_client.battery(series)))
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
    """Ground temperature: by depth rank (within each station of joined series, so a depth
    has the same colour at both stations); other rows: the palette in order."""
    if row.key == "ground":
        stations = list(dict.fromkeys(join.station(sel) for sel in row.items))
        if len(stations) < 2:
            return depth_colors(len(row.items))
        out = [None] * len(row.items)
        for st in stations:
            idx = [i for i, sel in enumerate(row.items) if join.station(sel) == st]
            for i, color in zip(idx, depth_colors(len(idx))):
                out[i] = color
        return out
    return [PALETTE[i % len(PALETTE)] for i in range(len(row.items))]


# ---- trace data -------------------------------------------------------------------------------

def _values(v) -> list:
    """Floats rounded to DECIMALS, None for NaN (a line break)."""
    out = np.round(np.asarray(v, dtype=float), DECIMALS).tolist()
    return [None if x != x else x for x in out]


def _daily_segment(t, arrays):
    t = t.astype("datetime64[D]")
    arrays = [np.asarray(a, dtype=float) for a in arrays]
    if len(t) > 1:
        idx = np.where(np.diff(t) > np.timedelta64(1, "D"))[0] + 1
        if len(idx):
            t = np.insert(t, idx, t[idx - 1] + np.timedelta64(1, "D"))
            arrays = [np.insert(a, idx, np.nan) for a in arrays]
    return t, arrays


def daily_trace_data(s) -> dict:
    """x (days), mean, min, max of a DailySeries, with a break (None) at every missing day
    and at every logger change of a joined series (a None point at the next logger's first
    day)."""
    ts, cols = [], [[], [], []]
    for i, (a, b) in enumerate(join.segments(s)):
        t, arrays = _daily_segment(s.time[a:b], [s.mean[a:b], s.min[a:b], s.max[a:b]])
        if i:
            t = np.insert(t, 0, t[0])
            arrays = [np.insert(x, 0, np.nan) for x in arrays]
        ts.append(t)
        for c, x in zip(cols, arrays):
            c.append(x)
    if not ts:
        return {"x": [], "mean": [], "min": [], "max": []}
    t = np.concatenate(ts)
    mean, lo, hi = (np.concatenate(c) for c in cols)
    return {"x": np.datetime_as_string(t, unit="D").tolist(),
            "mean": _values(mean), "min": _values(lo), "max": _values(hi)}


def raw_mode(n_points: int) -> str:
    return "lines+markers" if n_points <= RAW_MARKERS_MAX else "lines"


def raw_trace_data(sel, lo, hi, max_points: int | None = None) -> dict:
    """x, y and mode of one Full trace: the raw series of ``sel`` in [lo, hi], thinned to
    about ``max_points`` points (MAX_POINTS), plus ``bin_seconds`` (None: not thinned)."""
    s = sel.series
    xs, ys, bins = [], [], []
    for a, b in join.segments(s):       # a joined series: each logger's segment on its own
        t, v, _thinned, bin_s = rawseries.thin_series(s.time[a:b], s.value[a:b], lo, hi,
                                                      max_points or MAX_POINTS)
        if bin_s is not None:
            bins.append(bin_s)
        if not len(t):
            continue
        x = np.datetime_as_string(t, unit="s").tolist()
        if xs:                          # a break at the logger change
            xs.append(x[0])
            ys.append(None)
        xs.extend(x)
        ys.extend(_values(v))
    return {"x": xs, "y": ys, "mode": raw_mode(sum(1 for y in ys if y is not None)),
            "bin_seconds": max(bins) if bins else None}


# ---- the figure -------------------------------------------------------------------------------

def _axis_suffix(r: int) -> str:
    return "" if r == 1 else str(r)


def data_figure(rows_: list, mode: str, revision: str, window=None, max_points=None, changes=None):
    """(figure, trace keys, bin seconds) of the rows. ``changes``: the logger changes of
    joined series (join.Joined.changes), drawn as dashed lines with the logger's name.
    ``mode`` "daily" (DailySeries) or
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
    stations = list(dict.fromkeys(join.station(sel) for row in rows_ for sel in row.items))
    by_station = len(stations) > 1      # joined series of several stations (Mast 9)
    for r, row in enumerate(rows_, start=1):
        legend = f"legend{_axis_suffix(r)}"
        unit = unit_text(row.unit)
        many = len(row.items) > 1 or by_station
        titled = set()
        for sel, color in zip(row.items, row_colors(row)):
            group = f"{row.key}|{sel.key}"
            common = dict(name=sel.label, legend=legend, legendgroup=group, showlegend=many)
            name, dash = sel.label, "solid"
            if by_station:      # a legend group per station in each row, titled with it
                st = join.station(sel)
                group = f"{row.key}|{st}"
                common["legendgroup"] = group
                if group not in titled:
                    titled.add(group)
                    common["legendgrouptitle"] = dict(text=st, font=dict(size=12, color=MUTED))
                name = texts.TRACE_STATION.format(name=sel.label, station=st)
                dash = "solid" if stations.index(st) == 0 else "dot"
            hover_unit = unit_text(sel.unit)
            if raw:
                d = raw_trace_data(sel, *window, max_points=max_points)
                if d["bin_seconds"] is not None:
                    bins.append(d["bin_seconds"])
                fig.add_trace(go.Scatter(
                    x=d["x"], y=d["y"], mode=d["mode"], meta=sel.key, connectgaps=False,
                    line=dict(color=color, width=1.4, dash=dash), marker=dict(color=color, size=4),
                    hovertemplate=texts.HOVER_RAW.format(name=name, unit=hover_unit),
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
                connectgaps=False, line=dict(color=color, width=1.5, dash=dash),
                hovertemplate=texts.HOVER_DAILY.format(name=name, unit=hover_unit),
                **common), row=r, col=1)
            keys.extend([None, None, sel.key])
        fig.update_yaxes(title=dict(text=unit, standoff=6), row=r, col=1)
    height = plot_px + MARGIN["t"] + MARGIN["b"]
    fig.update_layout(**_base_layout(height), margin=MARGIN, hovermode="x unified",
                      uirevision=revision)
    if changes:
        fig.update_layout(shapes=change_shapes(changes))
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
    for c in changes or []:                 # the logger names, above the top row
        fig.add_annotation(x=_change_x(c), xref="x", y=1, yref="paper", yanchor="bottom",
                           xanchor="left", text=texts.CHANGE_LABEL.format(logger=c["label"]),
                           showarrow=False, font=dict(size=11, color=CHANGE_COLOR),
                           name="logger-change")
    return fig, keys, (max(bins) if bins else None)


CHANGE_COLOR = "#495057"


def _change_x(change) -> str:
    return change["time"].replace("T", " ")


def change_shapes(changes) -> list:
    """Dashed vertical lines over all rows at the logger changes (join.Joined.changes)."""
    return [dict(type="line", xref="x", yref="paper", x0=_change_x(c), x1=_change_x(c), y0=0, y1=1,
                 line=dict(color=CHANGE_COLOR, width=1.2, dash="dash"), name="logger-change")
            for c in changes or []]


def _plotly_time(t) -> str:
    """A datetime as Plotly's axis string (UTC, no zone)."""
    if getattr(t, "tzinfo", None) is not None:
        t = t.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return t.strftime("%Y-%m-%d %H:%M:%S")


# ---- ground temperature profile (T10) ---------------------------------------------------------

PROFILE_HEIGHT = 460
PROFILE_MARGIN = dict(l=70, r=20, t=60, b=20)
MEAN_COLOR = "#1d4f91"          # the page's accent colour
ANNUAL_MAX_COLOR = "#d62728"
ANNUAL_MIN_COLOR = "#1f77b4"
DAY_BAND_ALPHA = 0.22
DAY_MARKER_COLOR = "rgba(29, 79, 145, 0.55)"
ENVELOPES = ("day", "annual", "both")


def profile_figure(day: dict, annual: dict | None, envelope: str, x_range, depth_max: float,
                   revision: str) -> go.Figure:
    """Temperature (x) against depth (y, negative down, m) of one day (profile.day_profile):
    the daily mean, with the day's min/max band (``envelope`` "day" or "both") and/or the
    annual envelope's max and min curves (profile.annual_envelope; "annual" or "both").
    Every trace has ``meta``: "day-band", "annual-max", "annual-min", "mean". The axes are
    fixed by ``x_range`` (°C) and ``depth_max`` (m), so they do not move while sliding;
    ``revision`` is the uirevision (the user's zoom stays while sliding)."""
    envelope = envelope if envelope in ENVELOPES else "day"
    fig = go.Figure()
    if envelope in ("annual", "both") and annual and annual["depths"]:
        y = [-d for d in annual["depths"]]
        for key, name, color in (("max", texts.PROFILE_ANNUAL_MAX, ANNUAL_MAX_COLOR),
                                 ("min", texts.PROFILE_ANNUAL_MIN, ANNUAL_MIN_COLOR)):
            fig.add_trace(go.Scatter(
                x=annual[key], y=y, name=name, meta=f"annual-{key}", mode="lines+markers",
                customdata=[f"{v:+0.2f} m" for v in y], line=dict(color=color, width=1.6, dash="dash"),
                marker=dict(color=color, size=5), hovertemplate=texts.HOVER_PROFILE.format(name=name)))
    if day["depths"]:
        y = [-d for d in day["depths"]]
        if envelope in ("day", "both"):
            fig.add_trace(go.Scatter(
                x=day["max"] + day["min"][::-1], y=y + y[::-1], name=texts.PROFILE_DAY_BAND,
                meta="day-band", mode="lines", fill="toself", line=dict(width=0, color=MEAN_COLOR),
                fillcolor=_rgba(MEAN_COLOR, DAY_BAND_ALPHA), hoverinfo="skip"))
        fig.add_trace(go.Scatter(
            x=day["mean"], y=y, name=texts.PROFILE_MEAN, meta="mean", mode="lines+markers",
            customdata=list(zip(day["labels"], day["min"], day["max"])),
            line=dict(color=MEAN_COLOR, width=2), marker=dict(color=MEAN_COLOR, size=6),
            hovertemplate=texts.HOVER_PROFILE_MEAN.format(name=texts.PROFILE_MEAN)))
    annotations = []
    if not day["depths"]:
        annotations.append(dict(text=texts.PROFILE_NO_DAY.format(day=day["day"]), x=0.5, y=0.5,
                                xref="paper", yref="paper", showarrow=False, font=dict(size=15, color=MUTED)))
    lo, hi = x_range or [-10, 10]
    pad = 0.04 * (hi - lo)
    axis = dict(showgrid=True, gridcolor=GRID, showline=True, linecolor=AXIS, mirror=True,
                ticks="outside", tickcolor=AXIS, zeroline=False)
    fig.update_layout(
        **_base_layout(PROFILE_HEIGHT), margin=PROFILE_MARGIN, hovermode="closest",
        xaxis=dict(axis, title=dict(text=texts.PROFILE_XAXIS), side="top", range=[lo - pad, hi + pad]),
        yaxis=dict(axis, title=dict(text=texts.PROFILE_YAXIS), range=[-1.08 * (depth_max or 1.0), 0]),
        legend=dict(x=0.01, y=0.01, xanchor="left", yanchor="bottom", bgcolor="rgba(255,255,255,0.85)",
                    bordercolor=GRID, borderwidth=1, font=dict(size=12)),
        shapes=[dict(type="line", xref="x", yref="paper", x0=0, x1=0, y0=0, y1=1,
                     line=dict(color=AXIS, width=1.2, dash="dot"), layer="below")],
        annotations=annotations, uirevision=revision)
    return fig


def day_marker(day: str) -> dict:
    """A shape marking one UTC day (``"YYYY-MM-DD"``) on every row of the data figure (the
    x axes are matched, the shape spans the paper height)."""
    start = dt.date.fromisoformat(day)
    end = start + dt.timedelta(days=1)
    return dict(type="rect", xref="x", yref="paper", x0=f"{start.isoformat()} 00:00:00",
                x1=f"{end.isoformat()} 00:00:00", y0=0, y1=1, fillcolor=DAY_MARKER_COLOR,
                opacity=0.35, line=dict(color=DAY_MARKER_COLOR, width=1), layer="below")
