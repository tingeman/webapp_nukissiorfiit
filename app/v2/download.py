"""Excel download of the phase 2 page (docs/plan/PLAN_api-v2.md, "Phase 2 design", Download;
T12). No Dash: callbacks.py calls ``export`` and sends the bytes with dcc.Download.

    year_span(loggers)                       -> (first year, last year) with data, or None
    in_years(loggers, from_year, to_year)    -> [(logger, first day, last day)] to export
    export(client, mast, values, from_year, to_year, budget_s)  -> Export (filename, bytes, ...)
    fetch(client, items, deadline)           -> [(logger, RawResult, DailyResult)]
    workbook(mast_name, from_year, to_year, selected, parts, now)  -> bytes of the .xlsx
    sheet_names(loggers)                     -> {logger value: (full sheet, daily sheet)}
    filename(mast_name, from_year, to_year)  -> "nuki_Mast_6_2025-2026.xlsx"

Requests (the plan's API table): the selected loggers one after the other (never two loggers'
requests in flight); per logger /raw for the selected calendar years (UTC) clipped to the
logger's period (its catalogue first..last day), in raw_chunk_years chunks, at most
max_parallel in flight (api_client.Client.raw), then /daily for the same days. A logger whose
period does not overlap the years is not asked. One deadline (``client.deadline_in(budget_s)``,
DOWNLOAD_TIME_BUDGET_S) is passed to every call: after it no chunk is started, and the
download fails with ApiError(texts.DOWNLOAD_TOO_LARGE). Nothing is cached.

The workbook (openpyxl, write-only, built in memory):

* ``Info``: mast, requested years, download time (UTC), the source, the selected loggers
  (label, selector, station, period, the days exported, the sheets), and one row per column
  of every data sheet (sheet, column letter, header, sensor, series, unit, depth/relative_z,
  datatype, sensor type, API key, a note).
* per exported logger ``<logger> full``: "Time (UTC)" and one column per series, one row per
  measurement time of the logger (a cell is empty where that series has no value), and
  ``<logger> daily``: "Date (UTC)" and per series its mean, min, max and count of the UTC day,
  as the API's /daily returns them (the same numbers as the page).

Values are numbers in the API's units: unlike the page, pressure is NOT converted to kPa (the
Info sheet says so). Times are Excel datetimes, naive UTC (the headers say UTC). Sheet names
are at most 31 characters, unique (Excel compares them without case), without []:*?/\\.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import io
import logging
import re
import time

import numpy as np
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from . import api_client, join, texts

logger = logging.getLogger("nuki.v2.download")

UTC = dt.timezone.utc
MAX_SHEET = 31                  # Excel's limit for a sheet name
SUFFIX_FULL, SUFFIX_DAILY = " full", " daily"
MAX_BASE = MAX_SHEET - len(SUFFIX_DAILY)
EXCEL_MAX_ROWS = 1_048_576
INFO_SHEET = "Info"
_BAD_SHEET_CHARS = re.compile(r"[\[\]:*?/\\]")
DAILY_STATS = ("mean", "min", "max", "count")


class DownloadError(Exception):
    """A download that cannot be made (str(e) is the text for the page)."""


@dataclasses.dataclass(frozen=True)
class Export:
    filename: str
    content: bytes          # the .xlsx
    loggers: list           # the catalogue loggers exported, in order
    requests: int           # API requests made
    api_bytes: int          # size of the API answers (JSON, after transfer decompression)
    seconds: float          # wall time of the whole export


# ---- selection ------------------------------------------------------------------------------

def _day(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def period(lg) -> tuple | None:
    """(first, last) dates of a catalogue logger, or None without data."""
    first, last = _day(lg.get("first") or lg.get("last")), _day(lg.get("last"))
    if first is None or last is None:
        return None
    return first, last


def year_span(loggers) -> tuple | None:
    """(first year, last year) with data of the loggers, or None."""
    periods = [p for p in (period(lg) for lg in loggers or []) if p]
    if not periods:
        return None
    return min(p[0].year for p in periods), max(p[1].year for p in periods)


def in_years(loggers, from_year: int, to_year: int) -> list:
    """[(logger, first day, last day)]: the loggers whose period overlaps the calendar years
    from_year..to_year, with that overlap, in the given order."""
    lo, hi = dt.date(from_year, 1, 1), dt.date(to_year, 12, 31)
    out = []
    for lg in loggers or []:
        p = period(lg)
        if p is None:
            continue
        a, b = max(lo, p[0]), min(hi, p[1])
        if a <= b:
            out.append((lg, a, b))
    return out


def filename(mast_name: str, from_year: int, to_year: int) -> str:
    """nuki_<mast>_<from>-<to>.xlsx, with only ASCII letters, digits, "-" and "_" in the mast."""
    mast = re.sub(r"[^A-Za-z0-9-]+", "_", mast_name or "").strip("_-") or "mast"
    return f"nuki_{mast}_{int(from_year)}-{int(to_year)}.xlsx"


# ---- sheet names ----------------------------------------------------------------------------

def _clean(name: str) -> str:
    name = _BAD_SHEET_CHARS.sub("_", name)
    return re.sub(r"\s+", " ", name).strip(" '")


def _base_candidates(lg) -> list:
    label = join.short_label(lg)
    m = re.fullmatch(r"(.*) \(([^()]*)\)", label)
    display, ident = (m.group(1), m.group(2)) if m else (label, "")
    out = [label]
    if ident:
        out += [f"{display} {ident}", f"{display} {ident[-8:]}",
                f"{display[:MAX_BASE - 1 - len(ident[-8:])]} {ident[-8:]}"]
    out.append(label[:MAX_BASE])
    return [c for c in (_clean(x) for x in out) if c]


def sheet_names(loggers) -> dict:
    """{logger value: (full sheet name, daily sheet name)}: "<logger> full" and "<logger>
    daily", the logger as its short label, shortened to fit 31 characters (display name and
    the last 8 characters of the serial or deveui, then cut), and made unique (also against
    "Info", without case) by a number."""
    used = {INFO_SHEET.lower()}
    out = {}
    for lg in loggers:
        base = next((c for c in _base_candidates(lg) if len(c) <= MAX_BASE), None) or "Logger"
        candidate, n = base, 1
        while any(f"{candidate}{s}".lower() in used for s in (SUFFIX_FULL, SUFFIX_DAILY)):
            n += 1
            tail = f" {n}"
            candidate = base[:MAX_BASE - len(tail)].rstrip(" '") + tail
        names = (candidate + SUFFIX_FULL, candidate + SUFFIX_DAILY)
        used.update(x.lower() for x in names)
        out[lg["value"]] = names
    return out


# ---- fetching -------------------------------------------------------------------------------

def fetch(client: api_client.Client, items, deadline=None) -> list:
    """[(logger, RawResult, DailyResult)] of ``items`` [(logger, first day, last day)]: one
    logger after the other, /raw then /daily of its days, each with ``deadline``. Raises
    ApiError (the loggers after a failure are not asked)."""
    out = []
    for lg, a, b in items:
        raw = client.raw(lg["value"], a, b, deadline=deadline)
        daily = client.daily(lg["value"], a, b, deadline=deadline)
        out.append((lg, raw, daily))
    return out


# ---- workbook -------------------------------------------------------------------------------

def _num(v):
    """A float cell value (None for NaN)."""
    f = float(v)
    return None if f != f else f


def header(details: dict) -> str:
    """The header of a series column: "<sensor> <series> (<unit>)", depth for a ground
    temperature ("GroundTemp01 GroundTemperature -0.10 m (C)")."""
    parts = [details.get("sensor_name") or "", details.get("timeseries_name") or details.get("key") or ""]
    if details.get("timeseries_name") == "GroundTemperature" and details.get("relative_z") is not None:
        parts.append(f"{float(details['relative_z']):+0.2f} m")
    text = " ".join(p for p in parts if p)
    unit = details.get("unit")
    return f"{text} ({unit})" if unit else text


def _note(details: dict) -> str:
    if details.get("timeseries_name") == "BarometricPressure":
        return texts.XLSX_NOTE_PRESSURE.format(unit=details.get("unit") or "?")
    return ""


def _column_row(sheet, col, head, details, stat=None):
    note = _note(details)
    if stat:
        note = (texts.XLSX_STAT[stat] + (" " + note if note else "")).strip()
    z = details.get("relative_z")
    return [sheet, get_column_letter(col), head, details.get("sensor_name"), details.get("timeseries_name"),
            details.get("unit"), None if z is None else float(z), details.get("datatype"),
            details.get("sensor_type"), details.get("key"), note]


def _full_rows(series: dict):
    """(times as datetimes, [per series: values aligned with the times, None where missing])."""
    if not series:
        return [], []
    axis = np.unique(np.concatenate([s.time for s in series.values()]))
    columns = []
    for s in series.values():
        col = np.full(len(axis), np.nan)
        col[np.searchsorted(axis, s.time)] = s.value
        columns.append(col)
    return axis.astype("datetime64[ms]").tolist(), columns


def _daily_rows(series: dict):
    """(days as dates, [per series: (mean, min, max, count) arrays aligned with the days])."""
    if not series:
        return [], []
    axis = np.unique(np.concatenate([s.time for s in series.values()]))
    columns = []
    for s in series.values():
        idx = np.searchsorted(axis, s.time)
        stats = []
        for name in DAILY_STATS:
            col = np.full(len(axis), np.nan)
            col[idx] = getattr(s, name)
            stats.append(col)
        columns.append(stats)
    return axis.astype("datetime64[D]").tolist(), columns


def _check_rows(n: int):
    if n + 1 > EXCEL_MAX_ROWS:
        raise DownloadError(texts.DOWNLOAD_TOO_LARGE)


def _write_full(wb, name, series, details_rows):
    ws = wb.create_sheet(name)
    ws.freeze_panes = "B2"
    ws.column_dimensions["A"].width = 20
    times, columns = _full_rows(series)
    _check_rows(len(times))
    heads = [texts.XLSX_TIME] + [header(s.details) for s in series.values()]
    details_rows.append([name, "A", texts.XLSX_TIME, None, None, None, None, None, None, None, texts.XLSX_TIME_NOTE])
    for i, s in enumerate(series.values()):
        details_rows.append(_column_row(name, i + 2, heads[i + 1], s.details))
    ws.append(heads)
    for r, t in enumerate(times):
        ws.append([t] + [_num(c[r]) for c in columns])
    return len(times)


def _write_daily(wb, name, series, details_rows):
    ws = wb.create_sheet(name)
    ws.freeze_panes = "B2"
    ws.column_dimensions["A"].width = 12
    days, columns = _daily_rows(series)
    _check_rows(len(days))
    heads = [texts.XLSX_DATE]
    details_rows.append([name, "A", texts.XLSX_DATE, None, None, None, None, None, None, None, texts.XLSX_DATE_NOTE])
    for s in series.values():
        for stat in DAILY_STATS:
            head = f"{header(s.details)} {stat}"
            heads.append(head)
            details_rows.append(_column_row(name, len(heads), head, s.details, stat))
    ws.append(heads)
    for r, d in enumerate(days):
        row = [d]
        for mean, lo, hi, count in columns:
            row += [_num(mean[r]), _num(lo[r]), _num(hi[r]),
                    None if count[r] != count[r] else int(count[r])]
        ws.append(row)
    return len(days)


def workbook(mast_name: str, from_year: int, to_year: int, selected: list, parts: list,
             now: dt.datetime | None = None) -> bytes:
    """The .xlsx of an export: ``selected`` the catalogue loggers chosen, ``parts``
    [(logger, RawResult, DailyResult, first day, last day)] of those exported (see the module
    docstring)."""
    now = (now or dt.datetime.now(UTC)).astimezone(UTC).replace(microsecond=0, tzinfo=None)
    wb = Workbook(write_only=True)
    info = wb.create_sheet(INFO_SHEET)
    names = sheet_names([p[0] for p in parts])
    exported = {p[0]["value"]: p for p in parts}
    details_rows: list = []
    data_sheets = []
    for lg, raw, daily, _a, _b in parts:
        full_name, daily_name = names[lg["value"]]
        data_sheets.append((full_name, raw.series, _write_full))
        data_sheets.append((daily_name, daily.series, _write_daily))
    for name, series, writer in data_sheets:
        writer(wb, name, series, details_rows)

    info.column_dimensions["A"].width = 26
    info.column_dimensions["B"].width = 40
    info.column_dimensions["C"].width = 44
    info.append([texts.XLSX_TITLE])
    info.append([texts.XLSX_MAST, mast_name])
    info.append([texts.XLSX_YEARS, texts.XLSX_YEARS_VALUE.format(start=from_year, end=to_year)])
    info.append([texts.XLSX_DOWNLOADED, now])
    info.append([texts.XLSX_SOURCE, texts.XLSX_SOURCE_VALUE])
    for line in texts.XLSX_NOTES:
        info.append([texts.XLSX_NOTE, line])
    info.append([])
    info.append([texts.XLSX_LOGGERS])
    info.append(list(texts.XLSX_LOGGER_HEADERS))
    for lg in selected:
        p = exported.get(lg["value"])
        if p is None:
            days, sheets = texts.XLSX_NOT_EXPORTED, ""
        else:
            days = texts.XLSX_DAYS.format(start=p[3].isoformat(), end=p[4].isoformat())
            sheets = ", ".join(names[lg["value"]])
        info.append([join.short_label(lg), lg.get("label"), lg["value"], lg.get("station"),
                     lg.get("first"), lg.get("last"), days, sheets])
    info.append([])
    info.append([texts.XLSX_COLUMNS])
    info.append(list(texts.XLSX_COLUMN_HEADERS))
    for row in details_rows:
        info.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---- the whole export -----------------------------------------------------------------------

def export(client: api_client.Client, mast: dict, values, from_year, to_year, budget_s: float,
           now: dt.datetime | None = None) -> Export:
    """The download of the loggers ``values`` (catalogue values; the mast's order is kept) of
    ``mast`` for the calendar years from_year..to_year. Raises ApiError (API problem or the
    time budget used up: texts.DOWNLOAD_TOO_LARGE) or DownloadError (nothing to export)."""
    t0 = time.perf_counter()
    try:
        from_year, to_year = int(from_year), int(to_year)
    except (TypeError, ValueError):
        raise DownloadError(texts.DOWNLOAD_CHOOSE_YEARS) from None
    if from_year > to_year:
        raise DownloadError(texts.DOWNLOAD_YEARS_REVERSED)
    wanted = set(values or [])
    selected = [lg for lg in mast.get("loggers") or [] if lg["value"] in wanted]
    if not selected:
        raise DownloadError(texts.DOWNLOAD_CHOOSE_LOGGERS)
    items = in_years(selected, from_year, to_year)
    if not items:
        raise DownloadError(texts.DOWNLOAD_NO_DATA.format(start=from_year, end=to_year))
    deadline = client.deadline_in(budget_s)
    fetched = fetch(client, items, deadline)
    parts = [(lg, raw, daily, a, b) for (lg, raw, daily), (_lg, a, b) in zip(fetched, items)]
    content = workbook(mast["name"], from_year, to_year, selected, parts, now)
    n_requests = sum(len(r.requests) + len(d.requests) for _lg, r, d in fetched)
    api_bytes = sum(r.bytes + d.bytes for _lg, r, d in fetched)
    seconds = time.perf_counter() - t0
    logger.info("v2 download %s %d-%d: %d logger(s), %d request(s), %d bytes from the API, "
                "xlsx %d bytes, %.2f s", mast["name"], from_year, to_year, len(items), n_requests,
                api_bytes, len(content), seconds)
    return Export(filename(mast["name"], from_year, to_year), content, [lg for lg, _a, _b in items],
                  n_requests, api_bytes, seconds)
