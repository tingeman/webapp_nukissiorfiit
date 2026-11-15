"""Full-resolution ground temperature series from monitoring_db raw measurements.

Ported from webapp_grl_tsp/app/rawseries.py (f71f219, 2026-11-14) for the phase 2 page.
Changes: trumpet is imported relatively (package v2), and the per-series core of ``thin``
is public as ``thin_series`` (numpy arrays in and out), so any column (battery, weather,
inclination, ...) can be thinned, not only the ground temperatures. ``thin`` gives the
same result as in TSP.

Pure computation (numpy only): no Dash, no HTTP. Input is one or several JSON payloads of
``GET /api/v2/measurements/raw`` (columnar: ``time`` ISO strings, ``columns`` as in daily,
``values`` one array per column aligned with ``time``, null = missing). Values are used as
the API serves them.

Public interface
----------------
``parse_raw(payloads) -> RawData``
    One payload (a dict) or several (an iterable, in time order) are concatenated in the
    given order. Sensors: the same rule as ``trumpet.parse_daily`` (``trumpet.ground_sensors``:
    GroundTemperature, ``relative_z <= 0``, first column per depth, shallow to deep). Columns
    may differ between chunks (union by key; a chunk without a column gives NaN there). If
    the concatenated times are not ascending they are stably sorted.
    ``RawData``: ``times`` (``datetime64[ms]``, UTC), ``keys``, ``depths`` (m, positive
    down), ``values`` (float array sensors x times, NaN = no data).

``thin_series(t, v, start, end, max_points=2000) -> (t, v, thinned, bin_seconds)``
    The rule of ``thin`` (below) for one series: ``t`` ``datetime64[ms]`` ascending, ``v``
    floats (NaN ignored). Returns the kept points with the line breaks inserted (NaN values
    at the break times), whether it was thinned, and the bin width in seconds (None if not
    thinned).

``thin(raw, start, end, max_points=2000) -> Thinned``
    ``start``/``end`` (inclusive; ISO string, ``datetime``, or ``datetime64``; naive = UTC,
    a trailing ``Z`` is accepted) select the window. Per sensor, NaN values are ignored;
    if the sensor has at most ``max_points`` measurements in the window, they are returned
    unchanged (``thinned`` False). Otherwise the window is split into ``max_points // 2``
    equal time bins, and per bin the minimum and the maximum measurement are kept at their own
    timestamps, in time order (a one-sample spike survives; a bin with one measurement
    gives one point).
    Line breaks (a point with ``None`` value, placed inside the gap, so a line drawn without
    ``connectgaps`` breaks): in thinned series between two consecutive kept points that are
    more than one bin apart (an empty bin in between); in unthinned series where the gap
    between consecutive measurements is more than ``max(10 x the sensor's median interval
    in the window, 1 h)``. A shorter hole (e.g. a single missing hourly value) does not break.

    ``Thinned``: ``series`` (list, one dict per sensor, shallow to deep) and ``bin_seconds``
    (width of one bin in seconds when any sensor was thinned, else None; the label for T3:
    "min/max per ~<bin_seconds>").
    Each series dict: ``{"key", "depth", "dates", "mean", "min", "max", "thinned"}``;
    ``dates`` are ISO datetimes ``"YYYY-MM-DDTHH:MM:SS"`` (UTC, no zone suffix), ``mean``
    the values rounded to 3 decimals (None at breaks), ``min`` and ``max`` lists of None of
    the same length (the shape ``figures.timeseries_figure`` takes; there is no per-point
    min/max in raw mode). A sensor without any measurement in the window is kept as an
    empty series (empty lists, ``thinned`` False), as ``trumpet.timeseries`` keeps a sensor
    without data.
"""
from __future__ import annotations

import dataclasses
import datetime
from typing import Mapping

import numpy as np

from . import trumpet

DECIMALS = trumpet.DECIMALS
MAX_POINTS = 2000
GAP_FACTOR = 10                    # break an unthinned line at gaps > this x median interval ...
MIN_GAP = np.timedelta64(1, "h")   # ... but never for gaps shorter than this
_MS = np.timedelta64(1, "ms")


@dataclasses.dataclass(frozen=True)
class RawData:
    """Raw measurements of the ground temperature sensors of one device.

    ``times``: ``datetime64[ms]`` (UTC, ascending). ``keys``, ``depths``: one entry per
    sensor, shallow to deep. ``values``: float array (sensors x times), NaN = no data.
    """
    times: np.ndarray
    keys: tuple
    depths: np.ndarray
    values: np.ndarray


@dataclasses.dataclass(frozen=True)
class Thinned:
    """Result of ``thin``: ``series`` (one dict per sensor) and ``bin_seconds`` (or None)."""
    series: list
    bin_seconds: float | None


def _times(strings) -> np.ndarray:
    """ISO strings (trailing Z allowed) to datetime64[ms]."""
    return np.array([s[:-1] if s.endswith("Z") else s for s in strings], dtype="datetime64[ms]")


def _stamp(t) -> np.datetime64:
    if isinstance(t, datetime.datetime):
        if t.tzinfo is not None:
            t = t.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return np.datetime64(t, "ms")
    if isinstance(t, str):
        return np.datetime64(t[:-1] if t.endswith("Z") else t, "ms")
    return np.datetime64(t, "ms")


def parse_raw(payloads) -> RawData:
    """Concatenates /api/v2/measurements/raw payloads into one RawData (see module doc)."""
    if isinstance(payloads, Mapping):
        payloads = [payloads]
    payloads = list(payloads)
    keys, depth_of = trumpet.ground_sensors(payloads)
    row_of = {k: r for r, k in enumerate(keys)}

    chunk_times = [_times(p.get("time") or []) for p in payloads]
    total = sum(len(t) for t in chunk_times)
    times = np.concatenate(chunk_times) if chunk_times else np.array([], dtype="datetime64[ms]")
    values = np.full((len(keys), total), np.nan)
    offset = 0
    for p, t in zip(payloads, chunk_times):
        vals = p.get("values") or []
        for i, key, _ in trumpet._ground_columns(p.get("columns") or []):
            r = row_of.get(key)
            if r is not None and len(t):
                values[r, offset:offset + len(t)] = np.array(vals[i], dtype=float)
        offset += len(t)

    if len(times) > 1 and (np.diff(times) < np.timedelta64(0, "ms")).any():
        order = np.argsort(times, kind="stable")
        times, values = times[order], values[:, order]
    return RawData(times=times, keys=tuple(keys),
                   depths=np.array([depth_of[k] for k in keys], dtype=float), values=values)


def _series(key, depth, t, v, thinned, breaks) -> dict:
    """Series dict from points (t, v); ``breaks``: (position, time) pairs of None points to
    insert before position (positions ascending, relative to t)."""
    t, v = _with_breaks(t, v, breaks)
    mean = np.round(v, DECIMALS).tolist()
    for i in np.where(np.isnan(v))[0]:
        mean[i] = None
    return {"key": key, "depth": trumpet._r(depth),
            "dates": np.datetime_as_string(t, unit="s").tolist(),
            "mean": mean, "min": [None] * len(mean), "max": [None] * len(mean),
            "thinned": thinned}


def _window(start, end, max_points):
    t0, t1 = _stamp(start), _stamp(end)
    nbins = max(max_points // 2, 1)
    width = (t1 - t0) / np.timedelta64(1, "ms") / nbins    # bin width in ms
    return t0, t1, nbins, width


def _thin_points(t, v, t0, nbins, width, max_points):
    """(t, v, thinned, breaks) of one series inside the window (NaN already removed)."""
    if len(t) <= max_points or width <= 0:
        breaks = []
        if len(t) > 1:
            gaps = np.diff(t)
            limit = max(GAP_FACTOR * np.median(gaps / _MS), MIN_GAP / _MS)   # ms
            idx = np.where(gaps / _MS > limit)[0] + 1
            breaks = [(int(i), t[i - 1] + (t[i] - t[i - 1]) // 2) for i in idx]
        return t, v, False, breaks
    b = np.minimum(((t - t0) / _MS / width).astype(np.int64), nbins - 1)
    # sorted by (bin, value): first and last of each bin are its min and max
    order = np.lexsort((v, b))
    sb = b[order]
    first = np.r_[True, sb[1:] != sb[:-1]]
    last = np.r_[sb[1:] != sb[:-1], True]
    keep = np.unique(np.concatenate([order[first], order[last]]))   # ascending = time order
    kt, kv, kb = t[keep], v[keep], b[keep]
    jump = np.where(np.diff(kb) > 1)[0] + 1       # an empty bin between two kept points
    breaks = [(int(j), t0 + np.timedelta64(int((kb[j - 1] + 1) * width), "ms")) for j in jump]
    return kt, kv, True, breaks


def _with_breaks(t, v, breaks):
    if breaks:
        pos = np.array([b[0] for b in breaks])
        t = np.insert(t, pos, np.array([b[1] for b in breaks], dtype="datetime64[ms]"))
        v = np.insert(v, pos, np.nan)
    return t, v


def thin_series(t, v, start, end, max_points: int = MAX_POINTS):
    """One series of the window [start, end], thinned to min/max per bin if dense; NaN points
    mark the line breaks. Returns (t, v, thinned, bin_seconds or None)."""
    t0, t1, nbins, width = _window(start, end, max_points)
    t = np.asarray(t, dtype="datetime64[ms]")
    v = np.asarray(v, dtype=float)
    ok = (t >= t0) & (t <= t1) & ~np.isnan(v)
    kt, kv, thinned, breaks = _thin_points(t[ok], v[ok], t0, nbins, width, max_points)
    kt, kv = _with_breaks(kt, kv, breaks)
    return kt, kv, thinned, (width / 1000.0 if thinned else None)


def thin(raw: RawData, start, end, max_points: int = MAX_POINTS) -> Thinned:
    """Series of the window [start, end] per sensor, thinned to min/max per bin if dense."""
    t0, t1, nbins, width = _window(start, end, max_points)
    in_win = (raw.times >= t0) & (raw.times <= t1)
    wt = raw.times[in_win]
    out, any_thinned = [], False
    for r, key in enumerate(raw.keys):
        v_all = raw.values[r, in_win]
        ok = ~np.isnan(v_all)
        kt, kv, thinned, breaks = _thin_points(wt[ok], v_all[ok], t0, nbins, width, max_points)
        any_thinned = any_thinned or thinned
        out.append(_series(key, raw.depths[r], kt, kv, thinned, breaks))
    return Thinned(series=out, bin_seconds=width / 1000.0 if any_thinned else None)
