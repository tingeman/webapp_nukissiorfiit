"""Data of the ground temperature profile of the phase 2 page (docs/plan/PLAN_api-v2.md, T10).
Pure computation (numpy, no Dash, no HTTP): the load callback turns the daily ground
temperature series it already has into a compact, JSON-serialisable ``profile`` store, and
the profile callback draws any day and envelope from that store alone, so moving the day
slider or changing the envelope makes no API request.

    store_data(selected, logger_value)  -> the store (api_client.ground_temperature(...) of
                                           a DailyResult's series)
    day_profile(store, day)             -> that day's mean/min/max per depth
    annual_envelope(store, day)         -> min/max of the daily means over the 365 days up to
                                           the day (trumpet.trumpet, "means" mode)
    slider_marks(first, last)           -> marks at month (or quarter, year) starts

Days are integers: days since 1970-01-01 (UTC), so the slider's value is the day itself and
its tooltip can show the date without knowing the store (assets/profile.js).

The store (lists aligned with ``keys``, shallow to deep; per depth one value per calendar day
from ``first`` to ``first + n - 1``, None where there is no data; temperatures rounded to
DECIMALS):

    {"logger": <logger value>, "keys": [...], "labels": ["-0.10 m", ...],
     "depths": [0.1, ...] (m, positive down), "first": <day>, "n": <days>,
     "mean": [[...], ...], "min": [[...], ...], "max": [[...], ...],
     "count": [[...], ...] (measurements per day, 0 without data),
     "x_range": [lo, hi] (°C, whole degrees, all days: the axis stays put while sliding)}

A logger (or period) without ground temperatures gives ``{"logger", "keys": []}``; a failed
profile request in Full mode adds ``"error": <page text>``. Size: 4 numbers per depth and
day (about 25 bytes in JSON), e.g. about 90 KB for 5 depths over two years.
"""
from __future__ import annotations

import datetime as dt
import math

import numpy as np

from . import trumpet

DECIMALS = 2                 # °C; the sensors' accuracy is about 0.1 °C
WINDOW_DAYS = 365            # the annual envelope: the 365 days up to the chosen day
# trumpet.trumpet works on hydrological years (1 August to 31 July). The annual envelope's
# window is relabelled as the last days of hydrological year 2001 (2001-08-01..2002-07-31,
# 365 days, no 29 February), so that trumpet.py is used unchanged.
_ENVELOPE_YEAR = 2001
_ENVELOPE_END = np.datetime64("2002-07-31", "D")
EPOCH = dt.date(1970, 1, 1)


def day_number(d) -> int:
    """Days since 1970-01-01 of a date, datetime64 or ISO string."""
    if isinstance(d, str):
        d = dt.date.fromisoformat(d[:10])
    if isinstance(d, dt.date):
        return (d - EPOCH).days
    return int(np.datetime64(d, "D").astype(np.int64))


def day_text(day: int) -> str:
    return (EPOCH + dt.timedelta(days=int(day))).isoformat()


def _rounded(a: np.ndarray) -> list:
    out = np.round(a, DECIMALS).tolist()
    return [None if x != x else x for x in out]


def empty(logger_value, error: str | None = None) -> dict:
    out = {"logger": logger_value, "keys": []}
    if error:
        out["error"] = error
    return out


def store_data(selected: list, logger_value) -> dict:
    """The profile store of the ground temperature selection (api_client.Selected with a
    DailySeries each, shallow to deep, as api_client.ground_temperature returns them)."""
    items = [s for s in selected if len(s.series.time)]
    if not items:
        return empty(logger_value)
    days = [s.series.time.astype("datetime64[D]").astype(np.int64) for s in items]
    first = int(min(d[0] for d in days))
    n = int(max(d[-1] for d in days)) - first + 1
    out = {"logger": logger_value, "keys": [s.key for s in items], "labels": [s.label for s in items],
           "depths": [round(-float(s.series.details["relative_z"]), 3) for s in items],
           "first": first, "n": n, "mean": [], "min": [], "max": [], "count": []}
    lo, hi = math.inf, -math.inf
    for s, d in zip(items, days):
        idx = d - first
        for name in ("mean", "min", "max"):
            a = np.full(n, np.nan)
            a[idx] = getattr(s.series, name)
            out[name].append(_rounded(a))
        c = np.zeros(n, dtype=np.int64)
        c[idx] = s.series.count
        out["count"].append(c.tolist())
        lo, hi = min(lo, float(np.nanmin(s.series.min))), max(hi, float(np.nanmax(s.series.max)))
    lo, hi = math.floor(lo), math.ceil(hi)
    out["x_range"] = [lo, hi if hi > lo else lo + 1]
    return out


def has_profile(store) -> bool:
    return bool(store) and bool(store.get("keys"))


def last_day(store) -> int:
    return store["first"] + store["n"] - 1


def clamp_day(store, day) -> int:
    """``day`` within the store's days (the last day if None or not a number)."""
    try:
        day = int(day)
    except (TypeError, ValueError):
        return last_day(store)
    return min(max(day, store["first"]), last_day(store))


def day_profile(store, day: int) -> dict:
    """{"day", "depths", "labels", "mean", "min", "max"} of one day: the depths with data on
    that day only (empty lists for a day without data)."""
    i = day - store["first"]
    out = {"day": day_text(day), "depths": [], "labels": [], "mean": [], "min": [], "max": []}
    if not 0 <= i < store["n"]:
        return out
    for r, depth in enumerate(store["depths"]):
        mean = store["mean"][r][i]
        if mean is None:
            continue
        out["depths"].append(depth)
        out["labels"].append(store["labels"][r])
        out["mean"].append(mean)
        out["min"].append(store["min"][r][i] if store["min"][r][i] is not None else mean)
        out["max"].append(store["max"][r][i] if store["max"][r][i] is not None else mean)
    return out


def _matrix(rows, sl, dtype=float):
    return np.array([row[sl] for row in rows], dtype=dtype)


def annual_envelope(store, day: int) -> dict | None:
    """Maximum and minimum of the daily means per depth over the WINDOW_DAYS days up to and
    including ``day`` (fewer at the start of the record), by trumpet.trumpet in "means" mode:
    a day counts for a depth if it has at least 90 % of that depth's usual number of
    measurements per day in the window. Returns {"start", "end", "days" (window length),
    "valid_days" (most valid days of any depth), "depths", "max", "min"} (depths with a
    valid day only), or None if the day is outside the store."""
    hi = day - store["first"]
    if not 0 <= hi < store["n"]:
        return None
    lo = max(hi - WINDOW_DAYS + 1, 0)
    sl = slice(lo, hi + 1)
    m = hi - lo + 1
    daily = trumpet.DailyData(
        days=_ENVELOPE_END - np.arange(m - 1, -1, -1).astype("timedelta64[D]"),
        keys=tuple(store["keys"]),
        depths=np.array(store["depths"], dtype=float),
        mean=_matrix(store["mean"], sl), min=_matrix(store["min"], sl), max=_matrix(store["max"], sl),
        count=_matrix(store["count"], sl, np.int64))
    t = trumpet.trumpet(daily, _ENVELOPE_YEAR, "means")
    return {"start": day_text(day - m + 1), "end": day_text(day), "days": m,
            "valid_days": max(t["valid_days"], default=0),
            "depths": t["depths"], "max": t["max"], "min": t["min"]}


MONTHS_ALL = 430         # up to about 14 months: a mark at every month start
MONTHS_QUARTER = 3 * 366  # up to about 3 years: at quarter starts; longer: at year starts


def slider_marks(first: int, last: int) -> dict:
    """Slider marks {day: label}: month starts ("Jan" shown as the year), thinned to quarters
    or years for long records so that two years stay readable; the first mark also names
    its year. A record within one month gets its first and last day instead."""
    span = last - first
    step = 1 if span <= MONTHS_ALL else 3 if span <= MONTHS_QUARTER else 12
    d0 = EPOCH + dt.timedelta(days=first)
    y, mo = d0.year, d0.month
    marks = {}
    while True:
        mo += 1
        if mo > 12:
            y, mo = y + 1, 1
        day = day_number(dt.date(y, mo, 1))
        if day > last:
            break
        if (mo - 1) % step == 0:
            marks[day] = str(y) if mo == 1 else dt.date(y, mo, 1).strftime("%b")
    if marks and not any(label.isdigit() for label in marks.values()):
        k = min(marks)
        marks[k] = f"{marks[k]} {(EPOCH + dt.timedelta(days=k)).year}"
    if len(marks) < 2:
        fmt = lambda d: (EPOCH + dt.timedelta(days=d)).strftime("%d %b").lstrip("0")  # noqa: E731
        marks.setdefault(first, fmt(first))
        marks.setdefault(last, fmt(last))
    return {int(k): v for k, v in sorted(marks.items())}
