"""Data of the ground temperature profile of the phase 2 page (docs/plan/PLAN_api-v2.md, T10,
T16). Pure computation (numpy, no Dash, no HTTP): the load callback turns the daily ground
temperature series it already has into a compact, JSON-serialisable ``profile`` store that
holds everything the browser needs to draw any day and envelope (assets/profile.js, a
clientside callback): moving the day slider or changing the envelope makes no request, not
even to the page's own server.

    store_data(selected, logger_value)   -> the store (api_client.ground_temperature(...) of
                                            a DailyResult's series)
    matrices(selected)                   -> the daily arrays (rows x days) the store is made of
    rolling_envelope(mean, count)        -> the annual envelope of every day at once (numpy)
    trumpet_envelope(m, day_index, rows) -> the same for one day by trumpet.trumpet (the
                                            reference the tests compare rolling_envelope with)
    day_stations(stations, count)        -> the station each day's profile uses (Mast 9)
    slider_marks(first, last)            -> marks at month (or quarter, year) starts

Days are integers: days since 1970-01-01 (UTC), so the slider's value is the day itself and
its tooltip can show the date without knowing the store (assets/profile.js).

The store (row lists aligned with ``keys``, shallow to deep; per row one value per calendar
day from ``first`` to ``first + n - 1``, None where there is no value; temperatures rounded
to DECIMALS):

    {"logger": <logger value>, "keys": [...], "labels": ["-0.10 m", ...],
     "depths": [0.1, ...] (m, positive down), "first": <day>, "n": <days>,
     "mean", "min", "max": the day's mean, minimum and maximum (min/max = the mean where the
         API gave none),
     "amax", "amin": the annual envelope of the day: max and min of the valid daily means of
         the WINDOW_DAYS days up to and including the day (fewer at the start of the
         record); a day is valid for a depth if it has at least 90 % of the depth's most
         frequent daily count in that window (trumpet.py's "means" rule); None without a
         valid day,
     "valid": [<days>] per day: the most valid days of any (drawn) depth in its window,
     "x_range": [lo, hi] (°C, whole degrees, all days: the axis stays put while sliding),
     "depth_max": the deepest depth (m), "window": WINDOW_DAYS}

A logger (or period) without ground temperatures gives ``{"logger", "keys": []}``; a failed
profile request in Full mode adds ``"error": <page text>``. callbacks.py adds ``"draw"`` (the
figure template, figures.profile_spec) and ``"shapes"`` (the logger change lines of All
loggers, kept when the day marker is set).

All loggers (T11): the store is built from the joined ground temperatures (join.py: one
series per depth and station, spanning the loggers; on a day two loggers both recorded, the
later logger's value is kept). With several stations (Mast 9: two strings a few metres
apart) a day's profile and its annual envelope use one station only: the station with data
on that day (the one whose data starts later if both have), else the one with the latest
data before it. The store then also has ``"stations"`` (the station of each row),
``"station_list"`` (the stations) and ``"day_station"`` (per day an index into
station_list), and ``"valid"`` counts the day's station only.

Size: 5 numbers per depth and day (about 6 bytes each in JSON), e.g. about 110 KB for 5
depths and 220 KB for 10 depths over two years. It is sent to the browser once per load and
never back (no server callback takes it as an input or state).
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import math

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from . import join, trumpet

DECIMALS = 2                 # °C; the sensors' accuracy is about 0.1 °C
WINDOW_DAYS = 365            # the annual envelope: the 365 days up to the chosen day
# trumpet.trumpet works on hydrological years (1 August to 31 July). trumpet_envelope
# relabels the window as the last days of hydrological year 2001 (2001-08-01..2002-07-31,
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


@dataclasses.dataclass(frozen=True)
class Matrices:
    """The daily ground temperatures as arrays (rows x days, rows shallow to deep, days
    ``first`` .. ``first + n - 1``): ``mean`` rounded to DECIMALS (the values the page
    shows), ``min``/``max`` (the mean where missing), NaN without data; ``count`` 0 without
    data."""
    first: int
    n: int
    keys: tuple
    depths: np.ndarray
    mean: np.ndarray
    min: np.ndarray
    max: np.ndarray
    count: np.ndarray


def matrices(selected: list) -> Matrices | None:
    """The Matrices of the ground temperature selection (api_client.Selected with a
    DailySeries each), None without data."""
    items = [s for s in selected if len(s.series.time)]
    if not items:
        return None
    days = [s.series.time.astype("datetime64[D]").astype(np.int64) for s in items]
    first = int(min(d.min() for d in days))       # joined series: not sorted across loggers
    n = int(max(d.max() for d in days)) - first + 1
    shape = (len(items), n)
    mean, lo, hi = np.full(shape, np.nan), np.full(shape, np.nan), np.full(shape, np.nan)
    count = np.zeros(shape, dtype=np.int64)
    for r, (s, d) in enumerate(zip(items, days)):
        idx = d - first
        mean[r, idx] = s.series.mean
        lo[r, idx] = s.series.min
        hi[r, idx] = s.series.max
        count[r, idx] = s.series.count
    mean = np.round(mean, DECIMALS)
    lo = np.where(np.isnan(lo), mean, lo)
    hi = np.where(np.isnan(hi), mean, hi)
    depths = np.array([round(-float(s.series.details["relative_z"]), 3) for s in items])
    return Matrices(first, n, tuple(s.key for s in items), depths, mean, lo, hi, count)


# ---- the annual envelope ----------------------------------------------------------------------

def _rolling_nominal(count: np.ndarray, window: int) -> np.ndarray:
    """Per day, the most frequent non-zero daily count of the ``window`` days up to it (the
    larger one on a tie, as trumpet._nominal), 0 if none."""
    n = len(count)
    values = np.unique(count[count > 0])[::-1]          # descending: argmax takes the larger
    if not len(values):
        return np.zeros(n, dtype=np.int64)
    hits = (count[None, :] == values[:, None]).astype(np.int64)
    cum = np.concatenate([np.zeros((len(values), 1), dtype=np.int64), np.cumsum(hits, axis=1)], axis=1)
    end = np.arange(1, n + 1)
    freq = cum[:, end] - cum[:, np.maximum(end - window, 0)]
    best = np.argmax(freq, axis=0)
    return np.where(freq.max(axis=0) > 0, values[best], 0)


def _sliding(a: np.ndarray, window: int, fill: float) -> np.ndarray:
    """(n, window) view: row i is a[i - window + 1 .. i], padded with ``fill`` before a[0]."""
    return sliding_window_view(np.concatenate([np.full(window - 1, fill), a]), window)


def rolling_envelope(mean: np.ndarray, count: np.ndarray, window: int = WINDOW_DAYS):
    """(amax, amin, n_valid), each rows x days: per day the max and min of the valid daily
    means of the ``window`` days up to and including it (NaN without a valid day) and the
    number of valid days. Valid as trumpet.trumpet in "means" mode on that window: count > 0
    and count >= 0.9 x the window's most frequent non-zero count (per row). Vectorised per
    row and per distinct nominal count (a handful), so a record of years takes milliseconds."""
    rows, n = mean.shape
    amax, amin = np.full((rows, n), np.nan), np.full((rows, n), np.nan)
    n_valid = np.zeros((rows, n), dtype=np.int64)
    for r in range(rows):
        c, m = count[r], mean[r]
        nominal = _rolling_nominal(c, window)
        for v in np.unique(nominal[nominal > 0]):       # the days whose window has nominal v
            days = np.flatnonzero(nominal == v)
            valid = (c > 0) & (c >= trumpet.VALID_FRACTION * float(v)) & ~np.isnan(m)
            cum = np.concatenate([[0], np.cumsum(valid)])
            k = cum[days + 1] - cum[np.maximum(days + 1 - window, 0)]
            hi = _sliding(np.where(valid, m, -np.inf), window, -np.inf)[days].max(axis=1)
            lo = _sliding(np.where(valid, m, np.inf), window, np.inf)[days].min(axis=1)
            amax[r, days] = np.where(k > 0, hi, np.nan)
            amin[r, days] = np.where(k > 0, lo, np.nan)
            n_valid[r, days] = k
    return amax, amin, n_valid


def trumpet_envelope(m: Matrices, i: int, rows=None) -> dict:
    """The annual envelope of day index ``i`` (0 = ``m.first``) by trumpet.trumpet in "means"
    mode, on the window of WINDOW_DAYS days up to it (fewer at the start), for the given rows
    (default all; one station's rows, shallow to deep): {"rows": the rows with a valid day,
    "max", "min" (aligned with rows), "valid_days" (most valid days of any row)}. The
    reference for rolling_envelope (tests); the page uses the latter."""
    rows = list(range(len(m.keys))) if rows is None else list(rows)
    lo = max(i - WINDOW_DAYS + 1, 0)
    sl = slice(lo, i + 1)
    k = i - lo + 1
    daily = trumpet.DailyData(
        days=_ENVELOPE_END - np.arange(k - 1, -1, -1).astype("timedelta64[D]"),
        keys=tuple(m.keys[r] for r in rows), depths=m.depths[rows],
        mean=m.mean[rows, sl], min=m.min[rows, sl], max=m.max[rows, sl], count=m.count[rows, sl])
    t = trumpet.trumpet(daily, _ENVELOPE_YEAR, "means")
    with_data = [r for r in rows if round(float(m.depths[r]), 3) in t["depths"]]
    return {"rows": with_data, "max": t["max"], "min": t["min"],
            "valid_days": max(t["valid_days"], default=0)}


# ---- several stations (Mast 9) ----------------------------------------------------------------

def day_stations(stations: list, count: np.ndarray):
    """(station list, per day an index into it) of rows with several stations, or None: the
    station with data on the day (the one whose data starts later if both have), else the
    one with the latest data before it, else the one whose data starts first."""
    names = list(dict.fromkeys(stations))
    if len(names) < 2:
        return None
    n = count.shape[1]
    idx = np.arange(n)
    has = {st: (count[[r for r, s in enumerate(stations) if s == st]] > 0).any(axis=0) for st in names}
    start = {st: (int(np.flatnonzero(h)[0]) if h.any() else n) for st, h in has.items()}
    order = sorted(names, key=lambda st: start[st])     # stable: catalogue order on a tie
    best = np.full(n, names.index(order[0]), dtype=np.int64)
    best_i = np.full(n, -1)
    for st in order:            # by start; a later start wins a tie
        last = np.maximum.accumulate(np.where(has[st], idx, -1))    # latest data day <= i
        take = (last >= 0) & (last >= best_i)
        best = np.where(take, names.index(st), best)
        best_i = np.where(take, last, best_i)
    return names, best


# ---- the store ----------------------------------------------------------------------------------

def store_data(selected: list, logger_value) -> dict:
    """The profile store of the ground temperature selection (api_client.Selected with a
    DailySeries each, shallow to deep, as api_client.ground_temperature returns them)."""
    items = [s for s in selected if len(s.series.time)]
    m = matrices(items)
    if m is None:
        return empty(logger_value)
    amax, amin, n_valid = rolling_envelope(m.mean, m.count)
    out = {"logger": logger_value, "keys": list(m.keys), "labels": [s.label for s in items],
           "depths": m.depths.tolist(), "first": m.first, "n": m.n,
           "mean": [_rounded(a) for a in m.mean], "min": [_rounded(a) for a in m.min],
           "max": [_rounded(a) for a in m.max],
           "amax": [_rounded(a) for a in amax], "amin": [_rounded(a) for a in amin]}
    valid = n_valid.max(axis=0)
    if any(join.is_joined(s) for s in items):
        stations = [join.station(s) for s in items]
        out["stations"] = stations
        per_day = day_stations(stations, m.count)
        if per_day is not None:
            names, best = per_day
            out["station_list"], out["day_station"] = names, best.tolist()
            row_station = np.array([names.index(s) for s in stations])
            valid = np.where(row_station[:, None] == best[None, :], n_valid, 0).max(axis=0)
    out["valid"] = valid.tolist()
    lo, hi = math.floor(float(np.nanmin(m.min))), math.ceil(float(np.nanmax(m.max)))
    out["x_range"] = [lo, hi if hi > lo else lo + 1]
    out["depth_max"] = float(m.depths.max())
    out["window"] = WINDOW_DAYS
    return out


def has_profile(store) -> bool:
    return bool(store) and bool(store.get("keys"))


def last_day(store) -> int:
    return store["first"] + store["n"] - 1


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
