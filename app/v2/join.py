"""Joining the series of several loggers of one mast ("All loggers", docs/plan/PLAN_api-v2.md,
T11). Pure computation (numpy, no Dash, no HTTP): callbacks.py fetches the loggers one after
the other and hands their series here; figures.py and profile.py draw the joined series like
the series of one logger.

    join(parts)           -> Joined: one series per join key, the logger changes
    join_key(station, details)  -> the key that joins a column across loggers
    short_label(logger)   -> "<display name> (<serial or deveui>)" of a catalogue logger

``parts``: [(catalogue logger, series dict), ...], in the catalogue's order (by first
measurement); a series dict is the ``series`` of an api_client DailyResult or RawResult
(API column key -> DailySeries / RawSeries).

Join rule: columns are joined only within one station (a mast's loggers on different
stations, e.g. Mast 9's SIS2024-03 and NUKI2026-M9, are different strings a few metres apart,
so their series stay apart), and only with the same series name, datatype and unit (so a Pa
column is never joined with an hPa one); ground temperatures also need the same relative_z
(one line per depth). So Tekbox and Campbell battery, weather, heading and sensor temperature
series join by name; Tekbox Roll/Pitch and Campbell X/Y have different names and stay apart
(different instruments). If one logger has two columns with the same join key, the sensor
name is added to the key of those columns (they then join only with columns of the same
sensor name).

A joined series is a RawSeries/DailySeries of the concatenated arrays, in logger order. Its
details are the first logger's column details with ``key`` = the join key and three more
fields:

    "station"  the station identifier
    "loggers"  the logger values whose data it holds, in order
    "breaks"   indices into the arrays where a later logger's data starts: the figure breaks
               the line there (a gap, not a connecting line), whatever the time between

The arrays are ascending within each logger's segment; segments may overlap in time (two
loggers recording on the same day), so the whole array is not necessarily sorted.

Logger changes (``Joined.changes``): one per logger with data after the first logger with
data, at that logger's first measurement (earliest time of any of its series), in order:
{"time": "YYYY-MM-DDTHH:MM:SS" (UTC), "logger": value, "label": short_label, "station"}.
"""
from __future__ import annotations

import dataclasses

import numpy as np


@dataclasses.dataclass(frozen=True)
class Joined:
    series: dict        # join key -> RawSeries / DailySeries (details: see the module docstring)
    changes: list       # [{"time", "logger", "label", "station"}], one per later logger with data
    loggers: list       # the catalogue loggers of the parts that had data, in order


def short_label(logger: dict) -> str:
    """A catalogue logger's label without its period ("CR300_M6 (12096)"): the label is
    "<display name> (<serial or deveui>), <first> to <last>" (api_client._logger_entry)."""
    label = logger.get("label") or logger.get("value") or ""
    head, sep, _period = label.rpartition(", ")
    return head if sep else label


def join_key(station, details: dict, sensor: bool = False) -> str:
    """The join key of a column: station, series name, datatype, unit, and relative_z for a
    ground temperature (``sensor``: also the sensor name, for a repeated key)."""
    name = details.get("timeseries_name") or details.get("key")
    z = ""
    if name == "GroundTemperature" and details.get("relative_z") is not None:
        z = f"{float(details['relative_z']):g}"
    parts = [station or "", name, details.get("datatype") or "", details.get("unit") or "", z]
    if sensor:
        parts.append(details.get("sensor_name") or details.get("key") or "")
    return "|".join(parts)


def _keys(station, series: dict) -> dict:
    """API column key -> join key for one logger's series (repeated join keys get the sensor
    name)."""
    base = {k: join_key(station, s.details) for k, s in series.items()}
    counts: dict = {}
    for jk in base.values():
        counts[jk] = counts.get(jk, 0) + 1
    return {k: (jk if counts[jk] == 1 else join_key(station, series[k].details, sensor=True))
            for k, jk in base.items()}


def _has_data(s) -> bool:
    return len(s.time) > 0


def _concat(segments: list):
    """One series of the segments [(logger value, series)], with the join fields."""
    first = segments[0][1]
    breaks, n = [], 0
    for i, (_lg, s) in enumerate(segments):
        if i:
            breaks.append(n)
        n += len(s.time)
    fields = {f.name for f in dataclasses.fields(first)} - {"details"}
    arrays = {name: np.concatenate([getattr(s, name) for _lg, s in segments]) for name in fields}
    return dataclasses.replace(first, **arrays), breaks


def join(parts: list) -> Joined:
    """The joined series and the logger changes of ``parts`` (see the module docstring).
    Columns without data are left out."""
    groups: dict = {}       # join key -> [(logger value, series)], in logger order
    with_data = []
    for lg, series in parts:
        station = lg.get("station")
        series = {k: s for k, s in (series or {}).items() if _has_data(s)}
        if not series:
            continue
        with_data.append((lg, series))
        for k, jk in _keys(station, series).items():
            groups.setdefault(jk, []).append((lg["value"], series[k]))
    out = {}
    station_of = {lg["value"]: lg.get("station") for lg, _ in with_data}
    for jk, segments in groups.items():
        joined, breaks = _concat(segments)
        details = dict(segments[0][1].details)
        details.update(key=jk, station=station_of[segments[0][0]],
                       loggers=[v for v, _s in segments], breaks=breaks)
        out[jk] = dataclasses.replace(joined, details=details)
    changes = []
    for lg, series in with_data[1:]:
        t0 = min(np.datetime64(s.time[0], "s") for s in series.values())
        changes.append({"time": str(t0), "logger": lg["value"], "label": short_label(lg),
                        "station": lg.get("station")})
    return Joined(series=out, changes=changes, loggers=[lg for lg, _ in with_data])


def is_joined(sel) -> bool:
    """Whether an api_client.Selected (or a series) comes from join()."""
    series = getattr(sel, "series", sel)
    return "loggers" in (getattr(series, "details", None) or {})


def station(sel):
    """The station of a joined Selected (None for a single logger's series)."""
    series = getattr(sel, "series", sel)
    return (getattr(series, "details", None) or {}).get("station")


def segments(series) -> list:
    """[(lo, hi)] index ranges of a series' logger segments (one range without breaks)."""
    n = len(series.time)
    cuts = [0] + [b for b in (series.details.get("breaks") or []) if 0 < b < n] + [n]
    return [(a, b) for a, b in zip(cuts, cuts[1:]) if b > a]

