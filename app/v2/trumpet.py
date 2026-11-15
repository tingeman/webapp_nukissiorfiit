"""Ground temperature time series and trumpet curves from monitoring_db daily aggregates.

Ported unchanged from webapp_grl_tsp/app/trumpet.py (f71f219, 2026-11-14) for the phase 2
page (the profile and its annual envelope, T10). Keep the two copies in step.

Pure computation (numpy only): no Dash, no Flask, no HTTP. Input is one or several JSON
payloads of ``GET /api/v2/measurements/daily`` (one per requested chunk); output is plain
Python data (lists, floats, None), JSON-serialisable, for a ``dcc.Store``. The values are
used exactly as the API serves them: this module never applies calibrations or corrections.

Public interface
----------------
``parse_daily(payloads) -> DailyData``
    Merges one payload (a dict) or several (an iterable of dicts) into one daily table of
    the ground temperature sensors: columns with ``timeseries_name == "GroundTemperature"``
    and ``relative_z <= 0``; depth = ``-relative_z``; sensors sorted shallow to deep. Other
    columns (battery, logger or air temperature, sensors above ground, no ``relative_z``)
    are ignored. If two columns share a depth, the first in API order (payload order, then
    column order) wins. The same column and day in several payloads (a day split over two
    chunks) is merged: counts summed, mean weighted by count, min of the mins, max of the
    maxes. That is exact as long as the chunks do not overlap in time; a measurement that
    two chunks both include (inclusive ``end`` equal to the next ``start``) is counted twice.
    Request chunks with ``end`` = next ``start`` minus one second.

``timeseries(daily, max_gap_days=7) -> list[dict]``
    Plot arrays per sensor, shallow to deep: ``{"key", "depth", "dates", "mean", "min",
    "max"}``. ``dates`` are ``"YYYY-MM-DD"`` strings of the days with data. Where two
    consecutive data days are more than ``max_gap_days`` apart, a point with the day after
    the last data day and ``None`` values is inserted, so a line drawn without
    ``connectgaps`` breaks there.

``years(daily) -> list[int]``
    The hydrological years with any data, ascending. Year ``Y`` is the days ``Y-08-01`` to
    ``Y+1-07-31``, both inclusive (UTC days).

``trumpet(daily, year, mode="means") -> dict``
    The trumpet statistics of one year. ``mode`` is ``"means"`` (default, pypf) or
    ``"extremes"``:

    - A day is *valid* for a sensor in ``"means"`` mode if its ``count >= 0.9 * nominal``;
      ``nominal`` is the most frequent non-zero daily count of that sensor within the year
      (the larger one on a tie). This approximates pypf's ``calc_daily_average_adaptive``.
      In ``"extremes"`` mode every day with ``count > 0`` is valid.
    - Per depth, ``"means"``: max, min and mean of the valid daily means. ``"extremes"``:
      max of the daily max, min of the daily min, mean of the daily means. A depth with no
      valid day in the year is left out.
    - ``coverage`` per depth = valid days / days in the year (365 or 366); ``complete`` is
      True if every depth in the result has a coverage of at least 0.9.
    - ``alt``: pypf's ``find_zero(..., exclude_zeros=True)`` on the max curve, shallow to
      deep; the first crossing, and only if the max at the shallowest depth is > 0, else
      None.
    - ``zaa`` (always from the valid daily means, also in ``"extremes"`` mode, as pypf's
      ``get_zaa_stats``): the first depth, shallow to deep, where max - min <= 0.1 °C
      (``exact`` True), otherwise the deepest depth (``exact`` False); ``temperature`` is
      the mean at that depth. None if the year has no data.

    Returns ``{"year", "period" ("2021-2022"), "mode", "days_in_year", "depths", "max",
    "min", "mean", "valid_days", "coverage", "complete", "alt", "zaa": {"temperature",
    "depth", "exact"} or None}``; the lists are aligned with ``depths`` (shallow to deep).

``all_trumpets(daily) -> dict``
    Everything the page needs to redraw any year in either mode without a new request:
    ``{"years": [...], "means": {"<year>": trumpet, ...}, "extremes": {...},
    "x_range": {"means": [lo, hi] or None, "extremes": [lo, hi] or None},
    "depth_max": float or None, "default_year": int or None}``. Year keys are strings,
    so the structure survives a JSON round trip unchanged. ``depth_max`` is the deepest
    depth with data in any year (the trumpet y axis, constant while sliding).

``x_range(trumpets, mode=None) -> [lo, hi] or None``
    The overall min and max temperature over all years of an ``all_trumpets`` structure,
    of one mode (``"means"`` or ``"extremes"``) or of both (``mode=None``), rounded
    outward to multiples of 5 °C (pypf's rule), so the trumpet x axis does not change
    while sliding through the years. Per mode, because the daily extremes are wider (a
    full record of QAA2017-03 reaches about 35 °C). None if no data.

``default_year(trumpets) -> int or None``
    The latest year that is complete in ``"means"`` mode, else the latest year with data.

Rounding of the output: temperatures and depths 3 decimals, ALT 3, coverage 3.
"""
from __future__ import annotations

import dataclasses
import math
from typing import Iterable, Mapping

import numpy as np

MODES = ("means", "extremes")
VALID_FRACTION = 0.9      # pypf's threshold: share of the nominal daily count / of the year
ZAA_RANGE = 0.1           # °C, max - min of the daily means at the depth of zero annual amplitude
X_STEP = 5.0              # °C, the trumpet x axis is rounded outward to this step
DECIMALS = 3
YEAR_START_MONTH = 8      # hydrological year: August 1 to July 31


@dataclasses.dataclass(frozen=True)
class DailyData:
    """Daily aggregates of the ground temperature sensors of one device.

    ``days``: the UTC days in the payloads (``datetime64[D]``, ascending, unique).
    ``keys``, ``depths``: one entry per sensor, shallow to deep (depth in m, positive down).
    ``mean``, ``min``, ``max``: float arrays (sensors x days), NaN where there is no data.
    ``count``: int array (sensors x days), measurements per day, 0 where there is no data.
    """
    days: np.ndarray
    keys: tuple
    depths: np.ndarray
    mean: np.ndarray
    min: np.ndarray
    max: np.ndarray
    count: np.ndarray


# ---- parsing ---------------------------------------------------------------------------

def _ground_columns(columns) -> list[tuple[int, str, float]]:
    """(column index, key, depth) of the ground temperature columns of one payload."""
    out = []
    for i, c in enumerate(columns):
        rz = c.get("relative_z")
        if c.get("timeseries_name") != "GroundTemperature" or rz is None:
            continue
        rz = float(rz)
        if rz <= 0:
            out.append((i, c["key"], 0.0 - rz))
    return out


def ground_sensors(payloads) -> tuple[list[str], dict[str, float]]:
    """The ground temperature sensors of several payloads: (keys shallow to deep, depth by key).

    The one column-selection rule of the app (daily and raw): ``GroundTemperature`` with
    ``relative_z <= 0``; if several columns share a depth, the first in API order (payload
    order, then column order) wins.
    """
    depth_of: dict[str, float] = {}
    taken: set[float] = set()
    for p in payloads:
        for _, key, depth in _ground_columns(p.get("columns") or []):
            if key in depth_of or depth in taken:
                continue
            depth_of[key] = depth
            taken.add(depth)
    keys = sorted(depth_of, key=lambda k: depth_of[k])   # shallow to deep (depths are unique)
    return keys, depth_of


def _floats(values) -> np.ndarray:
    """JSON list (None for no data) to a float array (NaN for no data)."""
    return np.array(values, dtype=float)


def parse_daily(payloads) -> DailyData:
    """Merges /api/v2/measurements/daily payloads into one DailyData (see module doc)."""
    if isinstance(payloads, Mapping):
        payloads = [payloads]
    payloads = list(payloads)

    keys, depth_of = ground_sensors(payloads)
    row_of = {k: r for r, k in enumerate(keys)}

    all_days = [np.array(p.get("time") or [], dtype="datetime64[D]") for p in payloads]
    days = np.unique(np.concatenate(all_days)) if all_days else np.array([], dtype="datetime64[D]")
    n, m = len(keys), len(days)
    count = np.zeros((n, m), dtype=np.int64)
    wsum = np.zeros((n, m))
    vmin = np.full((n, m), np.inf)
    vmax = np.full((n, m), -np.inf)

    for p, pdays in zip(payloads, all_days):
        if not len(pdays):
            continue
        idx = np.searchsorted(days, pdays)
        vals = p.get("values") or {}
        for i, key, _ in _ground_columns(p.get("columns") or []):
            r = row_of.get(key)
            if r is None:
                continue
            mean = _floats(vals["mean"][i])
            cnt = np.nan_to_num(_floats(vals["count"][i])).astype(np.int64)
            ok = (cnt > 0) & ~np.isnan(mean)
            lo = _floats(vals["min"][i])
            hi = _floats(vals["max"][i])
            lo = np.where(np.isnan(lo), mean, lo)
            hi = np.where(np.isnan(hi), mean, hi)
            j = idx[ok]
            # idx is unique within one payload, so plain fancy-index updates are safe
            count[r, j] += cnt[ok]
            wsum[r, j] += mean[ok] * cnt[ok]
            vmin[r, j] = np.minimum(vmin[r, j], lo[ok])
            vmax[r, j] = np.maximum(vmax[r, j], hi[ok])

    has = count > 0
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(has, wsum / np.where(has, count, 1), np.nan)
    return DailyData(
        days=days, keys=tuple(keys), depths=np.array([depth_of[k] for k in keys], dtype=float),
        mean=mean, min=np.where(has, vmin, np.nan), max=np.where(has, vmax, np.nan), count=count)


# ---- helpers ---------------------------------------------------------------------------

def _r(v, decimals=DECIMALS):
    """Rounded Python float, None for NaN/None."""
    if v is None:
        return None
    v = float(v)
    return None if math.isnan(v) else round(v, decimals)


def _year_bounds(year: int) -> tuple[np.datetime64, np.datetime64]:
    """First and last day (inclusive) of hydrological year ``year``."""
    first = np.datetime64(f"{year:04d}-{YEAR_START_MONTH:02d}-01", "D")
    last = np.datetime64(f"{year + 1:04d}-{YEAR_START_MONTH:02d}-01", "D") - np.timedelta64(1, "D")
    return first, last


def _hydro_year(days: np.ndarray) -> np.ndarray:
    """Hydrological year of each day."""
    m = days.astype("datetime64[M]").astype(np.int64)    # months since 1970-01 (floor for earlier days)
    year, month = m // 12 + 1970, m % 12 + 1
    return np.where(month >= YEAR_START_MONTH, year, year - 1)


def _find_zero(xi, yi) -> list[float]:
    """pypf ``find_zero(xi, yi, exclude_zeros=True)``: depths of the sign changes of xi.

    A node that is exactly 0 is one crossing, at that node: [1, 0, -1] -> the depth of the
    0 node, once (the freezing front at the top of a 0 °C interval).
    """
    xi = np.asarray(xi, dtype=float)
    yi = np.asarray(yi, dtype=float)
    mydiff = np.diff(np.sign(xi))
    for i in range(len(mydiff)):
        if (xi[i] == 0 and xi[i + 1] < 0) or (xi[i] < 0 and xi[i + 1] == 0):
            mydiff[i] = 0
    ys = []
    for i in np.where(mydiff != 0)[0]:
        ys.append((yi[i] * (xi[i + 1] - 0) + yi[i + 1] * (0 - xi[i])) / (xi[i + 1] - xi[i]))
    return ys


def _nominal(count_row: np.ndarray) -> int:
    """Most frequent non-zero daily count (the larger on a tie), 0 if none."""
    nz = count_row[count_row > 0]
    if not len(nz):
        return 0
    vals, freq = np.unique(nz, return_counts=True)
    return int(vals[freq == freq.max()].max())


def _means_valid(count: np.ndarray) -> np.ndarray:
    """Valid days in "means" mode (90 % of the nominal count), sensors x days."""
    nominal = np.array([_nominal(row) for row in count], dtype=float)
    return (count > 0) & (count >= VALID_FRACTION * nominal[:, None])


def _reduce(how: str, a: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """max, min or mean of ``a`` over the valid days, per sensor; NaN for a sensor without one."""
    n = valid.sum(axis=1)
    if how == "mean":
        total = np.where(valid, a, 0.0).sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(n > 0, total / np.maximum(n, 1), np.nan)
    if how == "max":
        out = np.where(valid, a, -np.inf).max(axis=1, initial=-np.inf)
    else:
        out = np.where(valid, a, np.inf).min(axis=1, initial=np.inf)
    return np.where(n > 0, out, np.nan)


# ---- public functions ------------------------------------------------------------------

def timeseries(daily: DailyData, max_gap_days: int = 7) -> list[dict]:
    """Per-sensor plot arrays with a None point at gaps longer than max_gap_days."""
    out = []
    for r, key in enumerate(daily.keys):
        has = daily.count[r] > 0
        d = daily.days[has]
        mean, lo, hi = daily.mean[r, has], daily.min[r, has], daily.max[r, has]
        dates = np.datetime_as_string(d, unit="D").tolist()
        # no NaN on days with data (parse_daily fills min/max from the mean if missing)
        cols = [np.round(a, DECIMALS).tolist() for a in (mean, lo, hi)]
        if len(d) > 1:
            breaks = np.where(np.diff(d).astype(int) > max_gap_days)[0]
            for b in breaks[::-1]:   # insert from the end, so earlier positions stay valid
                dates.insert(b + 1, str(d[b] + np.timedelta64(1, "D")))
                for c in cols:
                    c.insert(b + 1, None)
        out.append({"key": key, "depth": _r(daily.depths[r]), "dates": dates,
                    "mean": cols[0], "min": cols[1], "max": cols[2]})
    return out


def years(daily: DailyData) -> list[int]:
    """Hydrological years with any data, ascending."""
    if not len(daily.days):
        return []
    has_day = (daily.count > 0).any(axis=0)
    return [int(y) for y in np.unique(_hydro_year(daily.days[has_day]))]


def trumpet(daily: DailyData, year: int, mode: str = "means") -> dict:
    """Trumpet statistics of hydrological year ``year`` (see module doc)."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    first, last = _year_bounds(year)
    days_in_year = int((last - first).astype(int)) + 1
    sel = (daily.days >= first) & (daily.days <= last)
    count = daily.count[:, sel]
    mean = daily.mean[:, sel]

    means_valid = _means_valid(count)
    if mode == "means":
        valid = means_valid
        tmax = _reduce("max", mean, valid)
        tmin = _reduce("min", mean, valid)
    else:
        valid = count > 0
        tmax = _reduce("max", daily.max[:, sel], valid)
        tmin = _reduce("min", daily.min[:, sel], valid)
    tmean = _reduce("mean", mean, valid)
    n_valid = valid.sum(axis=1)

    rows = np.where(n_valid > 0)[0]           # sensors are sorted shallow to deep
    depths = daily.depths[rows]
    coverage = n_valid[rows] / days_in_year

    alt = None
    if len(rows) and tmax[rows[0]] > 0:
        z0 = _find_zero(tmax[rows], depths)
        if z0:
            alt = z0[0]

    zaa = None
    zrows = np.where(means_valid.any(axis=1))[0]
    if len(zrows):
        zmax = _reduce("max", mean, means_valid)[zrows]
        zmin = _reduce("min", mean, means_valid)[zrows]
        zmean = _reduce("mean", mean, means_valid)[zrows]
        flat = np.where(zmax - zmin <= ZAA_RANGE + 1e-9)[0]
        k = int(flat[0]) if len(flat) else len(zrows) - 1
        zaa = {"temperature": _r(zmean[k]), "depth": _r(daily.depths[zrows[k]]),
               "exact": bool(len(flat))}

    return {
        "year": int(year), "period": f"{year}-{year + 1}", "mode": mode,
        "days_in_year": days_in_year,
        "depths": [_r(v) for v in depths],
        "max": [_r(v) for v in tmax[rows]],
        "min": [_r(v) for v in tmin[rows]],
        "mean": [_r(v) for v in tmean[rows]],
        "valid_days": [int(v) for v in n_valid[rows]],
        "coverage": [_r(v) for v in coverage],
        "complete": bool(len(rows)) and bool((coverage >= VALID_FRACTION).all()),
        "alt": _r(alt),
        "zaa": zaa,
    }


def x_range(trumpets: dict, mode: str | None = None):
    """[lo, hi] over all years of one mode (None: both), rounded outward to 5 °C; None if no data."""
    if mode is not None and mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    lo, hi = math.inf, -math.inf
    for m in MODES if mode is None else (mode,):
        for t in trumpets.get(m, {}).values():
            if t["min"]:
                lo = min(lo, min(t["min"]))
            if t["max"]:
                hi = max(hi, max(t["max"]))
    if lo == math.inf:
        return None
    lo, hi = math.floor(lo / X_STEP) * X_STEP, math.ceil(hi / X_STEP) * X_STEP
    if lo == hi:
        hi = lo + X_STEP
    return [lo, hi]


def default_year(trumpets: dict):
    """Latest year complete in "means" mode, else the latest year; None if no years."""
    ys = list(trumpets.get("years", []))
    if not ys:
        return None
    means = trumpets.get("means", {})
    complete = [y for y in ys if means.get(str(y), {}).get("complete")]
    return max(complete) if complete else max(ys)


def all_trumpets(daily: DailyData) -> dict:
    """Trumpets of every year with data, both modes, plus x ranges, depth_max, default_year."""
    ys = years(daily)
    out = {"years": ys}
    for mode in MODES:
        out[mode] = {str(y): trumpet(daily, y, mode) for y in ys}
    out["x_range"] = {mode: x_range(out, mode) for mode in MODES}
    depths = [d for t in out["means"].values() for d in t["depths"]]
    depths += [d for t in out["extremes"].values() for d in t["depths"]]
    out["depth_max"] = max(depths) if depths else None
    out["default_year"] = default_year(out)
    return out
