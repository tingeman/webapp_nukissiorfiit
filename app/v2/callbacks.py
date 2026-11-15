"""Callbacks of the phase 2 page (docs/plan/PLAN_api-v2.md, T9).

``register(app, client)`` attaches them with ``app.callback`` to the phase 2 Dash instance
only. Do not use the global ``dash.callback`` decorator here (or in app.py): with two Dash
apps on one server, Dash copies the global callbacks into the first app that sets itself up
(on the first request to the server, whatever its path), which is not necessarily the page
they belong to. Callbacks are plain sync functions (gunicorn runs sync workers); ``client``
is the process's one api_client.Client (its metadata caches live on it).

Data flow (requests per action in the plan, "Phase 2 design"):

1. Page load (``url``): the catalogue (``client.masts()``: v1 metadata and v2 metadata per
   station, cached by the client 10 minutes) into the ``masts`` store; the first mast.
2. Mast -> logger options, default the logger with the latest data (``default_logger``).
3. Logger -> date picker limits and range: the logger's first..last measurement day.
4. Logger, period or Daily/Full (``resolution``) -> ``load``: Daily is one /daily request
   set for the period (one line of daily means per series). Full is /raw for the period, at
   most one year (a longer period loads the year up to its end date, 365 or 366 days, and the
   info line says so; with BACKEND_API_RAW_CHUNK_YEARS=1 that is always one /raw request),
   thinned to about figures.MAX_POINTS points per series. The ``view`` store keeps what the
   figure shows: {"logger", "mode", "range": [lo, hi] ISO datetimes, "keys": the API column
   key of each trace (All loggers: the join key), "rows": figures.row_layout (the row titles
   and their trace keys), "note", "window": the zoomed x range [lo, hi] ISO or None, All
   loggers also "loggers": the mast's catalogue loggers}.
   The Daily/Full switch keeps a zoom (T16, as webapp_grl_tsp 24872f4): when the switch
   triggers the load and the view has a window for the same logger, Daily -> Full loads /raw
   for that window only (cut to the period and to at most its last year, full_period), and
   Full -> Daily loads the period as usual; both figures get the window as their x range.
   The Full ``range`` is then the year up to the window's end (what a reset loads).
5. Zoom or pan (``data-graph.relayoutData``, any x range change) in Full: /raw for the
   visible window only (one request set, exactly the window's datetimes, cut to the selected
   period, not to what was loaded before, and to at most its last year, with a note),
   thinned and patched into the traces by key (the figure's zoom stays as the user left
   it), the row titles too (thinned or not); the view's "range" becomes the window; a window
   outside the period makes no request. Autorange (reset) loads the first Full load again
   (the view's "home"; "period" holds the selected period). Daily makes no request on a zoom. Both
   record the window in the ``view`` store (a Patch). Nothing is kept on the server between
   requests.
6. The ground temperature profile (T10, T16): ``load`` also fills the ``profile`` store
   (profile.py: per depth and day the mean, min, max and annual envelope; figures.py: the
   figure template) and sets the day slider (limits, marks, default the last day) or hides
   the profile with a note. Daily reuses the load's /daily result; Full makes one extra
   /daily request set for its range, after the raw one. A zoom makes none. The profile is
   drawn in the browser (assets/profile.js, a clientside callback: no request, not even to
   this server, while the slider is dragged): the slider, the envelope choice or a new
   store draw it from the store alone and mark the day on the data figure (a Patch of its
   shapes). The store goes to the browser only: no server callback takes it as an input.

7. All loggers (T11): the logger dropdown's last option (``all:<mast name>``, only for a mast
   with 2 or more loggers). The date limits and default cover the union of the loggers'
   periods. The loggers whose period overlaps the range are fetched one after the other
   (never two loggers' requests in flight; an ApiError stops, the loggers after it are not
   fetched, and nothing half-joined is shown), each for its part of the range only, and
   joined by join.join (per station: ground temperature by depth, the rest by series name),
   with a dashed line at each logger change. Daily: one /daily request set per logger. Full:
   /raw for the one-year window (full_period) per logger with data in it, then the profile's
   /daily per such logger. The ``view`` store then also holds the mast's ``loggers``, and a
   zoom refetches /raw for the window per logger with data in it. The ``profile`` store comes
   from the joined ground temperatures (one station per day: profile.day_stations).

8. Download (T12, download.py): its own selectors. Mast -> the download checklist (the mast's
   loggers, all ticked, and "All" ticked); "All" ticks every logger or none, and is ticked
   exactly when every logger is. The ticked loggers -> the year dropdowns (options and
   default: their first..last year with data; reset on every change of the ticks). The
   button -> download.export: per ticked logger with data in the years, one after the other,
   /raw (its chunks at most max_parallel at once) then /daily of those days, under the time
   budget DOWNLOAD_TIME_BUDGET_S; the .xlsx goes to dcc.Download (dcc.send_bytes). The
   button is disabled while the download runs (``running=``). Problems (ApiError, also the
   used-up budget, or nothing to export) are shown next to the button.

Problems end up as an alert on the page (``str(ApiError)``, texts.py) and one log line,
never as a traceback on the page.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import time
import uuid

import dash_bootstrap_components as dbc
from dash import ClientsideFunction, Input, Output, Patch, State, ctx, dcc, html, no_update

from . import api_client, download, figures, join, profile, texts

logger = logging.getLogger("nuki.v2")

UTC = dt.timezone.utc


# ---- helpers (plain functions, no Dash state) -------------------------------------------------

def _alert(text: str, color: str):
    return dbc.Alert(text, color=color, className="nuki-alert mb-3", dismissable=False)


def size_text(n: int) -> str:
    if n < 1024 * 1024:
        return f"{max(n, 0) / 1024:.0f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


bin_text = figures.bin_text


def _date(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(UTC).replace(microsecond=0)


def find_mast(masts, name):
    return next((m for m in masts or [] if m["name"] == name), None)


ALL_PREFIX = "all:"


def all_value(mast_name: str) -> str:
    """The logger dropdown value of "All loggers" of a mast."""
    return ALL_PREFIX + mast_name


def is_all(value) -> bool:
    return isinstance(value, str) and value.startswith(ALL_PREFIX)


def logger_options(mast) -> list:
    """The logger dropdown options of a catalogue mast: its loggers, and "All loggers" last
    when it has 2 or more."""
    options = [{"label": lg["label"], "value": lg["value"]} for lg in mast["loggers"]]
    if len(mast["loggers"]) >= 2:
        options.append({"label": texts.LOGGER_ALL, "value": all_value(mast["name"])})
    return options


def logger_period(masts, value):
    """(first, last) "YYYY-MM-DD" of a logger value (All loggers: the union of the mast's
    loggers' periods), or None."""
    if is_all(value):
        mast = find_mast(masts, value[len(ALL_PREFIX):])
        loggers = [lg for lg in (mast or {}).get("loggers", []) if lg.get("last")]
        if not loggers:
            return None
        return (min((lg.get("first") or lg["last"])[:10] for lg in loggers),
                max(lg["last"][:10] for lg in loggers))
    lg = find_logger(masts, value)
    if lg is None or not lg.get("last"):
        return None
    return (lg.get("first") or lg["last"])[:10], lg["last"][:10]


def overlapping(loggers, start: dt.date, end: dt.date) -> list:
    """[(logger, first, last)] of the loggers whose period overlaps the days start..end, each
    with the overlap (dates), in the given (catalogue) order."""
    out = []
    for lg in loggers or []:
        first, last = _date(lg.get("first") or lg.get("last")), _date(lg.get("last"))
        if first is None or last is None:
            continue
        a, b = max(start, first), min(end, last)
        if a <= b:
            out.append((lg, a, b))
    return out


def fetch_each(fetch, items) -> list:
    """[(logger, result)] of ``fetch(logger value, start, end)`` for each (logger, start, end),
    one after the other; an ApiError propagates at once (the loggers after it are not
    fetched)."""
    out = []
    for lg, a, b in items:
        out.append((lg, fetch(lg["value"], a, b)))
    return out


def find_logger(masts, value):
    for m in masts or []:
        for lg in m["loggers"]:
            if lg["value"] == value:
                return lg
    return None


def full_period(start: dt.date, end: dt.date):
    """(start, end, limited) of a Full load: the period, or the year up to its end date
    (365 or 366 days: the day after ``end``, one year back), so it is one raw chunk."""
    after = end + dt.timedelta(days=1)
    try:
        lo = after.replace(year=after.year - 1)
    except ValueError:      # 29 February -> 1 March
        lo = after.replace(year=after.year - 1, month=3, day=1)
    if start < lo:
        return lo, end, True
    return start, end, False


def day_bounds(start: dt.date, end: dt.date):
    """(first, last) UTC datetimes of the days start..end (whole seconds), last at most now."""
    lo = dt.datetime(start.year, start.month, start.day, tzinfo=UTC)
    hi = dt.datetime(end.year, end.month, end.day, tzinfo=UTC) + dt.timedelta(days=1, seconds=-1)
    return lo, min(hi, _utc_now())


def parse_time(value) -> dt.datetime | None:
    """A Plotly axis value ('2021-08-03 12:34:56.789'; ISO with T or Z also) as UTC."""
    if not isinstance(value, str):
        return None
    text = value.strip().replace(" ", "T").removesuffix("Z")
    text, _, frac = text.partition(".")
    try:
        t = dt.datetime.fromisoformat(text + ("." + frac[:6].ljust(6, "0") if frac else ""))
    except ValueError:
        return None
    return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)


_X_RANGE0 = re.compile(r"^(xaxis\d*)\.range\[0\]$")
_X_RANGE = re.compile(r"^(xaxis\d*)\.range$")
_X_AUTO = re.compile(r"^xaxis\d*\.autorange$")


def zoom_target(relayout):
    """What a relayoutData of the figure asks for: "overview" (x autorange of any row), (lo,
    hi) UTC datetimes of a new x range (of any row: the x axes are matched), or None
    (anything else: autosize, a y axis only, ...)."""
    if not isinstance(relayout, dict):
        return None
    if any(_X_AUTO.match(k) and v is True for k, v in relayout.items()):
        return "overview"
    pair = None
    for k, v in relayout.items():
        m = _X_RANGE0.match(k)
        if m and f"{m.group(1)}.range[1]" in relayout:
            pair = [v, relayout[f"{m.group(1)}.range[1]"]]
            break
        if _X_RANGE.match(k):
            pair = v
            break
    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
        return None
    lo, hi = parse_time(pair[0]), parse_time(pair[1])
    if lo is None or hi is None:
        return None
    return (lo, hi) if lo <= hi else (hi, lo)


def clip_window(window, bounds):
    """The window cut to bounds (lo, hi), whole seconds, or None if nothing is left."""
    lo, hi = max(window[0], bounds[0]), min(window[1], bounds[1])
    lo, hi = lo.replace(microsecond=0), hi.replace(microsecond=0)
    return (lo, hi) if lo < hi else None


def _fmt_day(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d")


def _fmt_time(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M")


def info_items(what: str, result, note: str | None = None, profile_result=None):
    """The info line: what the figure shows, the API part (requests, wall seconds, size) and
    the requests one by one (folded). ``profile_result``: the extra /daily of a Full load
    (made after the raw one: the seconds add up); its requests are listed after the raw
    ones, each line with its kind."""
    if profile_result is None:
        return info_parts(what, [(None, result)], note)
    return info_parts(what, [(texts.INFO_KIND_RAW, result), (texts.INFO_KIND_PROFILE, profile_result)], note)


def info_parts(what: str, parts, note: str | None = None, loggers=None):
    """The info line of several results made one after the other: ``parts`` [(prefix or
    None, result)], each request line starting with its part's prefix (a kind, or for All
    loggers "<logger>, <kind>"); ``loggers``: the names of the loggers fetched (All loggers;
    an empty list says no logger has data in the period)."""
    requests = [(prefix, r) for prefix, res in parts for r in res.requests]
    api = texts.INFO_REQUESTS.format(requests=len(requests),
                                     seconds=sum(res.wall_seconds for _, res in parts),
                                     size=size_text(sum(res.bytes for _, res in parts)))
    lines = [html.Span(what, className="nuki-info-what"), html.Span(" · " + api, className="nuki-info-api")]
    out = [html.Div(lines)]
    if loggers is not None:
        out.append(html.Div(texts.INFO_LOGGERS.format(loggers="; ".join(loggers) or texts.INFO_NO_LOGGER),
                            className="nuki-info-loggers"))
    if note:
        out.append(html.Div(note, className="nuki-info-note"))
    out.append(html.Details([
        html.Summary(texts.INFO_DETAILS),
        html.Ul([html.Li((texts.INFO_REQUEST_KIND.format(kind=prefix) if prefix else "")
                         + texts.INFO_REQUEST_LINE.format(start=r.start, end=r.end, seconds=r.seconds,
                                                          size=size_text(r.bytes)))
                 for prefix, r in requests], className="nuki-requests"),
    ], className="nuki-details"))
    return out


def _prefixed(parts, kind=None) -> list:
    """[(prefix, result)] of All loggers' [(logger, result)]: "<logger>" or "<logger>, <kind>"."""
    return [(join.short_label(lg) + (f", {kind}" if kind else ""), res) for lg, res in parts]


def _full_what(lo, hi, bin_seconds, zoomed=False):
    if zoomed:
        what = texts.INFO_ZOOM.format(start=_fmt_time(lo), end=_fmt_time(hi))
        if bin_seconds is not None:
            what += " · " + texts.INFO_THINNED.format(start=_fmt_time(lo), end=_fmt_time(hi),
                                                       bin=bin_text(bin_seconds))
        return what
    if bin_seconds is None:
        return texts.INFO_FULL.format(start=_fmt_day(lo), end=_fmt_day(hi))
    return texts.INFO_THINNED.format(start=_fmt_day(lo), end=_fmt_day(hi), bin=bin_text(bin_seconds))


def keep_window(view, logger_value):
    """The zoomed x window (lo, hi) UTC datetimes of the ``view`` store, to keep across the
    Daily/Full switch, or None (not zoomed, or another logger)."""
    if not view or view.get("logger") != logger_value or not view.get("window"):
        return None
    lo, hi = (parse_time(t) for t in view["window"])
    return (lo, hi) if lo is not None and hi is not None and lo < hi else None


def zoomed_full(window, start: dt.date, end: dt.date):
    """(window, range, limited) of a Full load that keeps a zoom: ``window`` cut to the days
    start..end and to at most one year (its last year, as full_period; ``limited`` then
    True); ``range`` (lo, hi): the year up to the window's last day, within start..end (what
    autorange loads, and the profile's days). None if the window is outside the period."""
    w = clip_window(window, day_bounds(start, end))
    if w is None:
        return None
    first, _, limited = full_period(w[0].date(), w[1].date())
    if limited:
        w = (dt.datetime(first.year, first.month, first.day, tzinfo=UTC), w[1])
    a, b, _ = full_period(start, w[1].date())
    return w, day_bounds(a, b), limited


def profile_store(selected, logger_value, changes=None, source=None) -> dict:
    """The ``profile`` store (profile.store_data) with what the browser draws it with: the
    figure template (figures.profile_spec), the logger change lines (All loggers), which
    the day marker's Patch of the data figure keeps, and ``source`` (describe(): mast,
    station, logger), which starts the profile's label."""
    store = profile.store_data(selected, logger_value)
    if profile.has_profile(store):
        store["draw"] = figures.profile_spec(store, revision=str(logger_value))
        store["shapes"] = figures.change_shapes(changes)
        store["source"] = source
    return store


def find_mast_of(masts, logger_value):
    """The catalogue mast of a logger value (also "all:<mast>"), or None."""
    if is_all(logger_value):
        return find_mast(masts, logger_value[len(ALL_PREFIX):])
    return next((m for m in masts or [] if any(lg["value"] == logger_value for lg in m["loggers"])), None)


def describe(masts, logger_value, resolution=None, names=None) -> str:
    """What is drawn, without the period: "Mast 6 · SIS2024-01 · CR300_M6 (12096) · Daily
    means" (``resolution`` None: without the values part). All loggers: "All loggers:
    <names>" (``names``: the loggers fetched; default every logger of the mast) and the
    stations of the mast."""
    mast = find_mast_of(masts, logger_value)
    parts = [mast["name"]] if mast else []
    if is_all(logger_value):
        loggers = (mast or {}).get("loggers") or []
        stations = list(dict.fromkeys(lg.get("station") for lg in loggers if lg.get("station")))
        if names is None:
            names = [join.short_label(lg) for lg in loggers]
        who = texts.HEADING_ALL.format(loggers="; ".join(names) or texts.INFO_NO_LOGGER)
    else:
        lg = find_logger(masts, logger_value)
        stations = [lg["station"]] if lg and lg.get("station") else []
        who = join.short_label(lg) if lg else str(logger_value)
    parts += [", ".join(stations)] if stations else []
    parts.append(who)
    if resolution is not None:
        parts.append(texts.HEADING_FULL if resolution == "full" else texts.HEADING_DAILY)
    return texts.HEADING_SEP.join(parts)


def heading(what: str | None, lo=None, hi=None, problem: str | None = None, times=False):
    """The heading above the figure (``data-heading``): ``what`` (describe()), the period
    lo..hi (dates, or with ``times`` date and time), and a problem (nothing drawn)."""
    if not what:
        return None
    text = what
    if lo is not None and hi is not None:
        fmt = _fmt_time if times else (lambda t: t.isoformat()[:10])
        text += texts.HEADING_SEP + texts.HEADING_PERIOD.format(start=fmt(lo), end=fmt(hi))
    out = [html.Span(text, className="nuki-heading-what")]
    if problem:
        out.append(html.Span(texts.HEADING_SEP + problem, className="nuki-heading-problem"))
    return out


def _iso(window):
    return [t.isoformat() for t in window] if window else None


def load(client: api_client.Client, logger_value, start, end, resolution, masts=None, window=None):
    """Everything a logger/period/resolution change produces, as plain values: figure,
    info (children), message (alert or None), view (store or None), profile (store or
    None, profile_store). Raises ApiError (not for the profile request of a Full load: its
    error goes into the profile store, and the Full figure is shown). ``masts``: the
    catalogue (the ``masts`` store), needed for All loggers only. ``window``: (lo, hi) UTC
    datetimes of a zoom to keep (keep_window; the Daily/Full switch), else None. Also
    "heading": what the figure shows (heading()); without data the figure is empty and the
    heading says so."""
    out = {"figure": figures.empty_figure(texts.FIGURE_EMPTY), "info": None, "message": None,
           "view": None, "profile": None, "heading": None}
    if not logger_value:
        return out
    start, end = _date(start), _date(end)
    if start is None or end is None:
        out["message"] = _alert(texts.CHOOSE_PERIOD, "warning")
        return out
    if start > end:
        out["message"] = _alert(texts.PERIOD_REVERSED, "warning")
        return out
    if is_all(logger_value):
        mast = find_mast(masts, logger_value[len(ALL_PREFIX):])
        if mast is None or len(mast.get("loggers") or []) < 2:
            out["message"] = _alert(texts.CHOOSE_FIRST, "warning")
            return out
        return load_all(client, mast, logger_value, start, end, resolution, out, window)
    try:
        api_client.logger_params(logger_value)
    except ValueError:
        out["message"] = _alert(texts.CHOOSE_FIRST, "warning")
        return out
    revision = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    period = _iso(day_bounds(start, end))       # the selected period: a pan/zoom stays inside it
    if resolution == "full":
        zoomed = zoomed_full(window, start, end) if window else None
        if zoomed:              # the switch keeps the zoom: raw for the window only
            shown, (lo, hi), limited = zoomed
            note = texts.INFO_ZOOM_LIMIT if limited else None
            start, end = lo.date(), hi.date()
        else:
            start, end, limited = full_period(start, end)
            lo, hi = day_bounds(start, end)
            shown = (lo, hi)
            note = texts.INFO_FULL_LIMIT if limited else None
        result = client.raw(logger_value, *shown)
        rows = figures.rows(result.series)
        fig, keys, bin_s = figures.data_figure(rows, "raw", revision, window=shown)
        what = _full_what(*shown, bin_s, zoomed=bool(zoomed))
        mode = "full"
        profile_result = None
        try:        # the profile: one /daily request set for the days of the range
            profile_result = client.daily(logger_value, start, end)
            out["profile"] = profile_store(api_client.ground_temperature(profile_result.series), logger_value,
                                           source=describe(masts, logger_value))
        except api_client.ApiError as e:        # the client logged it
            out["profile"] = profile.empty(logger_value, str(e))
        window = shown if zoomed else None
    else:
        lo, hi = day_bounds(start, end)
        window = clip_window(window, (lo, hi)) if window else None
        result = client.daily(logger_value, start, end)
        rows = figures.rows(result.series)
        fig, keys, _ = figures.data_figure(rows, "daily", revision, x_range=window)
        note = None
        what = _daily_what(start, end, window)
        mode = "daily"
        profile_result = None       # the profile reuses this /daily result
        out["profile"] = profile_store(api_client.ground_temperature(result.series), logger_value,
                                       source=describe(masts, logger_value))
    logger.info("v2 load %s %s %s..%s: %d request(s), API %.2f s, %d bytes, %d rows, %d traces, %.2f s",
                logger_value, mode, lo.isoformat(), hi.isoformat(), len(result.requests),
                result.wall_seconds, result.bytes, len(rows), len(keys), time.perf_counter() - t0)
    title = describe(masts, logger_value, mode)
    shown_period = shown if mode == "full" else (start, end)
    if not rows:
        out["message"] = _alert(texts.NO_DATA.format(start=start.isoformat(), end=end.isoformat()), "info")
        out["info"] = info_items(what, result, note, profile_result)
        out["heading"] = heading(title, *shown_period, texts.HEADING_NO_DATA, times=bool(window))
        return out
    out["figure"] = fig
    out["info"] = info_items(what, result, note, profile_result)
    out["heading"] = heading(title, *shown_period, times=bool(window) and mode == "full")
    out["view"] = {"logger": logger_value, "mode": mode, "range": [lo.isoformat(), hi.isoformat()],
                   "keys": keys, "rows": figures.row_layout(rows), "note": note, "window": _iso(window),
                   "heading": title, "home": [lo.isoformat(), hi.isoformat()], "period": period}
    return out


def _daily_what(start: dt.date, end: dt.date, window=None) -> str:
    what = texts.INFO_DAILY.format(start=start.isoformat(), end=end.isoformat())
    if window:
        what += " · " + texts.INFO_ZOOM.format(start=_fmt_time(window[0]), end=_fmt_time(window[1]))
    return what


def window_items(loggers, window) -> list:
    """[(logger, lo, hi)] of the loggers with data in ``window`` (lo, hi) datetimes, each with
    its part of the window, in catalogue order."""
    items = []
    for lg in loggers or []:
        first, last = _date(lg.get("first") or lg.get("last")), _date(lg.get("last"))
        if first is None or last is None:
            continue
        part = clip_window(window, day_bounds(first, last))
        if part is not None:
            items.append((lg, *part))
    return items


def load_all(client: api_client.Client, mast, logger_value, start: dt.date, end: dt.date,
             resolution, out, window=None):
    """``load`` for All loggers of ``mast`` (see the module docstring, 7.)."""
    revision = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    period = _iso(day_bounds(start, end))       # the selected period: a pan/zoom stays inside it
    loggers = mast["loggers"]
    note = None
    if resolution == "full":
        mode = "full"
        zoomed = zoomed_full(window, start, end) if window else None
        if zoomed:              # the switch keeps the zoom: raw for the window only
            shown, (lo, hi), limited = zoomed
            note = texts.INFO_ZOOM_LIMIT if limited else None
            start, end = lo.date(), hi.date()
            raw_items = window_items(loggers, shown)
        else:
            start, end, limited = full_period(start, end)
            note = texts.INFO_FULL_LIMIT if limited else None
            lo, hi = day_bounds(start, end)
            shown = (lo, hi)
            raw_items = overlapping(loggers, start, end)
        items = overlapping(loggers, start, end)
        parts = fetch_each(client.raw, raw_items)
        joined = join.join([(lg, r.series) for lg, r in parts])
        rows = figures.rows(joined.series)
        fig, keys, bin_s = figures.data_figure(rows, "raw", revision, window=shown, changes=joined.changes)
        what = _full_what(*shown, bin_s, zoomed=bool(zoomed))
        info = _prefixed(parts, texts.INFO_KIND_RAW)
        # the profile: one /daily request set per logger with data in the range, in turn
        profile_parts = []
        try:
            for lg, a, b in items:
                profile_parts.append((lg, client.daily(lg["value"], a, b)))
            pj = join.join([(lg, r.series) for lg, r in profile_parts])
            out["profile"] = profile_store(api_client.ground_temperature(pj.series), logger_value, joined.changes,
                                           describe([mast], logger_value))
        except api_client.ApiError as e:        # the client logged it
            out["profile"] = profile.empty(logger_value, str(e))
        info += _prefixed(profile_parts, texts.INFO_KIND_PROFILE)
        window = shown if zoomed else None
    else:
        mode = "daily"
        lo, hi = day_bounds(start, end)
        window = clip_window(window, (lo, hi)) if window else None
        items = overlapping(loggers, start, end)
        parts = fetch_each(client.daily, items)
        joined = join.join([(lg, r.series) for lg, r in parts])
        rows = figures.rows(joined.series)
        fig, keys, _ = figures.data_figure(rows, "daily", revision, changes=joined.changes, x_range=window)
        what = _daily_what(start, end, window)
        info = _prefixed(parts)
        out["profile"] = profile_store(api_client.ground_temperature(joined.series), logger_value, joined.changes,
                                       describe([mast], logger_value))
    names = [join.short_label(lg) for lg, _ in parts]
    n_requests = sum(len(r.requests) for _, r in info)
    logger.info("v2 load %s %s %s..%s: %d logger(s), %d request(s), API %.2f s, %d bytes, %d rows, "
                "%d traces, %.2f s", logger_value, mode, lo.isoformat(), hi.isoformat(), len(parts),
                n_requests, sum(r.wall_seconds for _, r in info), sum(r.bytes for _, r in info),
                len(rows), len(keys), time.perf_counter() - t0)
    out["info"] = info_parts(what, info, note, names)
    title = describe([mast], logger_value, mode, names)
    shown_period = shown if mode == "full" else (start, end)
    times = bool(window) and mode == "full"
    if not rows:
        out["message"] = _alert(texts.NO_DATA_ALL.format(start=start.isoformat(), end=end.isoformat()), "info")
        out["heading"] = heading(title, *shown_period, texts.HEADING_NO_DATA, times=times)
        return out
    out["figure"] = fig
    out["heading"] = heading(title, *shown_period, times=times)
    out["view"] = {"logger": logger_value, "mode": mode, "range": [lo.isoformat(), hi.isoformat()],
                   "keys": keys, "rows": figures.row_layout(rows), "note": note, "window": _iso(window),
                   "loggers": loggers, "heading": title, "home": [lo.isoformat(), hi.isoformat()],
                   "period": period}
    return out


def zoom_all(client: api_client.Client, view, window):
    """(series, info parts, logger names) of a zoom with All loggers: /raw for the window per
    logger with data in it, one after the other, joined."""
    parts = fetch_each(client.raw, window_items(view["loggers"], window))
    joined = join.join([(lg, r.series) for lg, r in parts])
    return joined.series, _prefixed(parts, texts.INFO_KIND_RAW), [join.short_label(lg) for lg, _ in parts]


def zoom_window(target, view):
    """(window, limited) of a zoom or pan to ``target`` (lo, hi): cut to the selected period
    (``view["period"]``; older views: the loaded range) and, in Full, to at most one year
    (the year up to the window's end, as full_period; ``limited`` then True). None if the
    window is outside the period."""
    period = tuple(parse_time(t) for t in (view.get("period") or view["range"]))
    window = clip_window(target, period)
    if window is None or view["mode"] != "full":
        return (window, False) if window else None
    first, _, limited = full_period(window[0].date(), window[1].date())
    if limited:
        window = (dt.datetime(first.year, first.month, first.day, tzinfo=UTC), window[1])
    return window, limited


def zoom(client: api_client.Client, relayout, view):
    """What a relayout of the data figure changes, or None (no x range change, or a window
    outside the selected period): {"window": the new zoom window [lo, hi] ISO, None after
    autorange, and in Full "range": what is now loaded (for the view store); in Full also
    "patch" (the traces' x/y/mode replaced by key from /raw for the window, thinned; the row
    titles) and "info" and "heading" (the period now drawn)}. A zoom or pan in Full loads
    the visible window, anywhere in the selected period (zoom_window: a year at most);
    autorange goes back to the first Full load (``view["home"]``). Daily makes no request.
    Raises ApiError."""
    if not view or view.get("mode") not in ("daily", "full"):
        return None
    target = zoom_target(relayout)
    if target is None:
        return None
    note = None
    if target == "overview":
        window, zoomed = tuple(parse_time(t) for t in (view.get("home") or view["range"])), False
        note = view.get("note")
    else:
        clipped = zoom_window(target, view)
        if clipped is None:
            return None
        (window, limited), zoomed = clipped, True
        note = texts.INFO_ZOOM_LIMIT if limited else None
    out = {"window": _iso(window) if zoomed else None}
    if view["mode"] != "full":
        return out
    out["range"] = _iso(window)
    if view.get("loggers"):         # All loggers
        series, parts, names = zoom_all(client, view, window)
    else:
        result = client.raw(view["logger"], *window)
        series, parts, names = result.series, [(None, result)], None
    by_key = {sel.key: sel for row in figures.rows(series) for sel in row.items}
    patch = Patch()
    bins, bin_of = [], {}
    for i, key in enumerate(view["keys"]):
        if key is None:
            continue
        sel = by_key.get(key)
        if sel is None:
            d = {"x": [], "y": [], "mode": "lines", "bin_seconds": None}
        else:
            d = figures.raw_trace_data(sel, *window)
        if d["bin_seconds"] is not None:
            bins.append(d["bin_seconds"])
            bin_of[key] = d["bin_seconds"]
        patch["data"][i]["x"] = d["x"]
        patch["data"][i]["y"] = d["y"]
        patch["data"][i]["mode"] = d["mode"]
    for i, (title, keys) in enumerate(view.get("rows") or []):     # the row titles come first
        row_bins = [bin_of[k] for k in keys if k in bin_of]
        patch["layout"]["annotations"][i]["text"] = figures.row_title(
            title, "raw", max(row_bins) if row_bins else None)
    what = _full_what(window[0], window[1], max(bins) if bins else None, zoomed)
    logger.info("v2 zoom %s %s..%s: %d request(s), API %.2f s, %d bytes", view["logger"],
                window[0].isoformat(), window[1].isoformat(), sum(len(r.requests) for _, r in parts),
                sum(r.wall_seconds for _, r in parts), sum(r.bytes for _, r in parts))
    out.update(patch=patch, info=info_parts(what, parts, note, names),
               heading=heading(view.get("heading"), *window, times=zoomed) if view.get("heading") else None)
    return out


def profile_setup(store):
    """(slider min, max, marks, value, body style, note) for a new ``profile`` store: the
    loaded days, default the last one; without ground temperatures the body is hidden and
    the note says why."""
    hidden = {"display": "none"}
    if not store:
        return 0, 1, {}, None, hidden, texts.PROFILE_EMPTY
    if store.get("error"):
        return 0, 1, {}, None, hidden, _alert(texts.PROFILE_ERROR.format(reason=store["error"]), "warning")
    if not profile.has_profile(store):
        return 0, 1, {}, None, hidden, texts.PROFILE_NO_GROUND
    first, last = store["first"], profile.last_day(store)
    return first, last, profile.slider_marks(first, last), last, {}, None


# ---- download (T12) ---------------------------------------------------------------------------

XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DOWNLOAD_ALL = "all"


def download_ticks(trigger, mast, all_value, values):
    """(logger options, ticked logger values, "All" value) of the download checklists after
    ``trigger`` changed ("download-all", "download-loggers", else the mast: reset to all)."""
    if mast is None:
        return [], [], []
    options = [{"label": lg["label"], "value": lg["value"]} for lg in mast["loggers"]]
    every = [o["value"] for o in options]
    if trigger == "download-all":
        ticked = every if DOWNLOAD_ALL in (all_value or []) else []
    elif trigger == "download-loggers":
        ticked = [v for v in every if v in (values or [])]
    else:
        ticked = every
    return options, ticked, [DOWNLOAD_ALL] if every and len(ticked) == len(every) else []


def download_years(mast, values):
    """(year options, from year, to year) of the ticked loggers: their first..last year with
    data, default the whole span; ([], None, None) without data."""
    ticked = [lg for lg in (mast or {}).get("loggers", []) if lg["value"] in (values or [])]
    span = download.year_span(ticked)
    if span is None:
        return [], None, None
    return [{"label": str(y), "value": y} for y in range(span[0], span[1] + 1)], span[0], span[1]


def download_file(client: api_client.Client, masts, mast_name, values, from_year, to_year,
                  budget_s: float, now=None):
    """(dcc.Download data or no_update, message) of a click on the download button."""
    mast = find_mast(masts, mast_name)
    if mast is None:
        return no_update, _alert(texts.CHOOSE_FIRST, "warning")
    try:
        result = download.export(client, mast, values, from_year, to_year, budget_s, now)
    except download.DownloadError as e:
        return no_update, _alert(str(e), "warning")
    except api_client.ApiError as e:        # the client logged it
        return no_update, _alert(str(e), "danger")
    return (dcc.send_bytes(result.content, result.filename, type=XLSX_TYPE),
            texts.DOWNLOAD_DONE.format(filename=result.filename, size=size_text(len(result.content))))


# ---- registration -----------------------------------------------------------------------------

def register(app, client: api_client.Client, download_budget_s: float | None = None):
    """Register the page's callbacks on ``app`` (the v2 Dash instance), using ``client``.
    ``download_budget_s``: the download's time budget (default: DOWNLOAD_TIME_BUDGET_S of
    config.settings, read at each download)."""

    def budget() -> float:
        if download_budget_s is not None:
            return download_budget_s
        import config       # app/config.py (top-level module)
        return config.settings.download_time_budget_s

    @app.callback(
        Output("masts", "data"),
        Output("mast", "options"),
        Output("mast", "value"),
        Output("masts-message", "children"),
        Input("url", "pathname"),
    )
    def load_masts(_pathname):
        try:
            masts = client.masts()
        except api_client.ApiError as e:        # the client logged it
            return None, [], None, _alert(texts.MASTS_UNAVAILABLE.format(reason=str(e)), "danger")
        if not masts:
            return [], [], None, _alert(texts.NO_MASTS, "warning")
        options = [{"label": m["name"], "value": m["name"]} for m in masts]
        return masts, options, masts[0]["name"], None

    @app.callback(
        Output("logger", "options"),
        Output("logger", "value"),
        Input("mast", "value"),
        State("masts", "data"),
    )
    def fill_loggers(mast_name, masts):
        mast = find_mast(masts, mast_name)
        if mast is None:
            return [], None
        default = api_client.default_logger(mast)
        return logger_options(mast), default["value"] if default else None

    @app.callback(
        Output("period", "min_date_allowed"),
        Output("period", "max_date_allowed"),
        Output("period", "start_date"),
        Output("period", "end_date"),
        Output("period", "initial_visible_month"),
        Input("logger", "value"),
        State("masts", "data"),
    )
    def set_period(logger_value, masts):
        period = logger_period(masts, logger_value)
        if period is None:
            return None, None, None, None, None
        first, last = period
        return first, last, first, last, last

    @app.callback(
        Output("data-graph", "figure"),
        Output("info", "children"),
        Output("message", "children"),
        Output("view", "data"),
        Output("profile", "data"),
        Output("profile-day", "min"),
        Output("profile-day", "max"),
        Output("profile-day", "marks"),
        Output("profile-day", "value"),
        Output("profile-body", "style"),
        Output("profile-note", "children"),
        Output("data-heading", "children"),
        Input("logger", "value"),
        Input("period", "start_date"),
        Input("period", "end_date"),
        Input("resolution", "value"),
        State("masts", "data"),
        State("view", "data"),
    )
    def load_data(logger_value, start, end, resolution, masts, view):
        # the Daily/Full switch keeps a zoom; any other change shows the whole period
        window = keep_window(view, logger_value) if ctx.triggered_id == "resolution" else None

        def failed(message, reason):
            """Nothing drawn: an empty figure (never the previous selection's), the heading
            names the selection and the problem."""
            try:
                what = describe(masts, logger_value, resolution)
            except Exception:       # noqa: BLE001  (the heading must not hide the message)
                what = None
            return (figures.empty_figure(""), None, _alert(message, "danger"), None, None, *profile_setup(None),
                    heading(what, _date(start), _date(end), texts.HEADING_FAILED.format(reason=reason)))

        try:
            r = load(client, logger_value, start, end, resolution, masts, window)
            setup = profile_setup(r["profile"])
        except api_client.ApiError as e:        # the client logged it
            return failed(str(e), str(e))
        except Exception:   # a bug or an unexpected payload: message, log, page stays usable
            logger.exception("v2 load %s %s..%s (%s) failed", logger_value, start, end, resolution)
            return failed(texts.UNEXPECTED, texts.UNEXPECTED)
        return (r["figure"], r["info"], r["message"], r["view"], r["profile"], *setup, r["heading"])

    @app.callback(
        Output("data-graph", "figure", allow_duplicate=True),
        Output("info", "children", allow_duplicate=True),
        Output("message", "children", allow_duplicate=True),
        Output("view", "data", allow_duplicate=True),
        Output("data-heading", "children", allow_duplicate=True),
        Input("data-graph", "relayoutData"),
        State("view", "data"),
        prevent_initial_call=True,
    )
    def zoom_data(relayout, view):
        try:
            r = zoom(client, relayout, view)
        except api_client.ApiError as e:        # the client logged it
            return no_update, no_update, _alert(str(e), "danger"), no_update, no_update
        except Exception:
            logger.exception("v2 zoom failed (%s)", relayout)
            return no_update, no_update, _alert(texts.UNEXPECTED, "danger"), no_update, no_update
        if r is None:
            return no_update, no_update, no_update, no_update, no_update
        window = Patch()            # the zoom, for the Daily/Full switch; Full: what is loaded
        window["window"] = r["window"]
        if "range" in r:
            window["range"] = r["range"]
        if "patch" not in r:        # Daily: no request, nothing redrawn
            return no_update, no_update, no_update, window, no_update
        return r["patch"], r["info"], None, window, r["heading"] or no_update

    # the profile: drawn in the browser from the store (assets/profile.js), also while the
    # slider is dragged; no request
    app.clientside_callback(
        ClientsideFunction(namespace="nuki", function_name="profile"),
        Output("profile-graph", "figure"),
        Output("profile-label", "children"),
        Output("data-graph", "figure", allow_duplicate=True),
        Input("profile-day", "value"),
        Input("profile-envelope", "value"),
        Input("profile", "data"),
        prevent_initial_call=True,
    )

    @app.callback(
        Output("download-loggers", "options"),
        Output("download-loggers", "value"),
        Output("download-all", "value"),
        Input("mast", "value"),
        Input("download-all", "value"),
        Input("download-loggers", "value"),
        State("masts", "data"),
    )
    def tick_download_loggers(mast_name, all_value, values, masts):
        return download_ticks(ctx.triggered_id, find_mast(masts, mast_name), all_value, values)

    @app.callback(
        Output("download-from", "options"),
        Output("download-from", "value"),
        Output("download-to", "options"),
        Output("download-to", "value"),
        Input("download-loggers", "value"),
        State("mast", "value"),
        State("masts", "data"),
    )
    def set_download_years(values, mast_name, masts):
        options, first, last = download_years(find_mast(masts, mast_name), values)
        return options, first, options, last

    @app.callback(
        Output("download-file", "data"),
        Output("download-message", "children"),
        Input("download-button", "n_clicks"),
        State("mast", "value"),
        State("download-loggers", "value"),
        State("download-from", "value"),
        State("download-to", "value"),
        State("masts", "data"),
        running=[(Output("download-button", "disabled"), True, False),
                 (Output("download-button", "children"), texts.DOWNLOAD_RUNNING, texts.DOWNLOAD_BUTTON)],
        prevent_initial_call=True,
    )
    def make_download(_clicks, mast_name, values, from_year, to_year, masts):
        try:
            return download_file(client, masts, mast_name, values, from_year, to_year, budget())
        except Exception:   # a bug or an unexpected payload: message, log, page stays usable
            logger.exception("v2 download %s %s-%s %s failed", mast_name, from_year, to_year, values)
            return no_update, _alert(texts.UNEXPECTED, "danger")

    return app
